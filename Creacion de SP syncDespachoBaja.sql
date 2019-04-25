USE TwinsDb
GO
/****** Object:  StoredProcedure [dbo].[syncDespacho]    Script Date: 06/10/2014 14:10:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--set xact_abort on
--go


IF not EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'syncDespachoBaja')
   exec('CREATE Procedure dbo.syncDespachoBaja  AS BEGIN SET NOCOUNT ON; END')
GO


alter Procedure [dbo].[syncDespachoBaja] (
		@TWNcRem	int,
		@modoDebug	int=0
)
as
Begin
/*
15/10/2012
Jaír Hnatiuk
Interfaz Physis-Twins

Para anular un comprobante se usa el stored 
                      spFACComprobantes_Anular [IdCabecera], [FechaAnulacion]

Tenes q validar primero q se pueda anular (Ej: no tenga referenciados se verifica con spFACTieneReferenciados [IdCabecera], tenga permisos, sea del origen q lo quiere borrar, o el remito no tenga un viaje q ya fue facturado, etc)

Mensajes q pueden salir cuando anulas desde sifac
"No puede anular el comprobante. El viaje está facturado en: " + CComprobante.Viaje.FacturaTransportista, vbInformation, "Atención"
"No puede anular el comprobante. Ya se ha emitido la Factura de Comisiones de este comprobante.", vbInformation, "Atención"
"No puede anular el comprobante porque existe un asiento" + vbCrLf + "de costo con fecha igual o posterior al mismo."
"El comprobante ya está anulado."

** Agregamos una opcion: si hay un punto de venta predeterminado, se admite replicar la baja de un despacho
** 23/7/15

*/
	Set nocount on
	declare @IdEjercicio	smallint  
	Declare @TWRemitoNro	varchar(10)
	Declare @IdCabecera	int
	Declare	@TWFecha		varchar(8)
	Declare	@Fecha			smalldatetime
	declare	@baseHija		varchar(200)
	-- para sql dinamico	
	Declare	@sql			nvarchar(max)
	Declare	@param			nvarchar(max)
	-- para contar los comprobantes
	Declare	@cuenta			smallint
	-- manejo de errores
	DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
	-- datos de las bases physis
	Declare	@BasesPhy	table	(
		proceso			int,
		twPuntoVenta	varchar(20),
		phyBase			varchar(200),
		phyCompro		varchar(20),
		phySucursal		varchar(20),
		phyTipoCompro	varchar(5),
		IdPlanPpal		smallint
	)
	Declare	Proceso	cursor for
		select proceso, twPuntoVenta, phyBase, phyCompro, phySucursal, phyTipoCompro, IdPlanPpal
		from @BasesPhy
	-- y las variables para recorrer eso
	Declare	@Vproceso int,
		@VtwPuntoVenta	varchar(20),
		@vphyBase	varchar(200),
		@VphyCompro	varchar(20),
		@VphySucursal	varchar(20),
		@VphyTipoCompro	varchar(5),
		@VIdPlanPpal	smallint
	-- Punto de venta default
	Declare	@CONSTPuntoVentaDefault	varchar(20)
	-- lo cargamos, si existe
	Select  @CONSTPuntoVentaDefault=valor
	From	dbo.syncInterfazGralTw
	where opcion='puntoVentaDefault'

	-- Anulo o elimino?
	Declare	@elimino	bit	-- si es cero, anulo, si es uno: elimino
	Select	@elimino=valor
	From	dbo.syncInterfazGralTw
	where	opcion='EliminaAlAnular'

	-- Valido que exista parametrizacion para el caso:
	if isnull(	(Select count(1)
			From dbo.syncDespachosConfig SDC, dbo.[remitos resumen] RR
			where SDC.idUsuario = RR.nc_u -- modificado 1/7/16. no usan mas usuarioFaena, sino nc_u 
				and (SDC.twPuntoVenta=RR.empresa collate Modern_Spanish_CI_AS 
				or SDC.twPuntoVenta=RR.sucursal collate Modern_Spanish_CI_AS 
				or ((rr.empresa = '' and @CONSTPuntoVentaDefault = SDC.twPuntoVenta) 
					or (rr.sucursal = '' and @CONSTPuntoVentaDefault = SDC.twPuntoVenta) )
				) and RR.nc_rem = @TWNcRem
		),0) < 1
	begin
		-- Si no tengo el "caminito" del remito, hasta aqui llegó mi amor
		select @ErrorMessage =  'SDB. Imposible hallar parametrizacion para procesar la baja del remito NC_REM:' + cast(@TWNcRem as varchar)
		raiserror (@ErrorMessage,16,1)
		return -1
	end	
	-- si todo anduvo bien, empiezo a recabar info:
	-- primero del remito
	-- Notar que si no hay numero de remito, lo genero con el nro de carga y ceros
	Select	@TWRemitoNro	= case when RemitoNro='' then right(cast('00000000' as varchar) + cast(nc_rem as varchar),8)
								  else RemitoNro end,
			@TWFecha		= fecha
	From	dbo.[Remitos resumen]
	Where	Nc_rem	= @TWNcRem 
	-- y ahora de las bases Physis donde ese remito fue a parar:
	if @modoDebug > 0
		Print 'SDB. El nro de comprobante es: ' + ltrim(rtrim(cast(@TWRemitoNro as varchar)))
	insert @BasesPhy
		Select distinct SDC.idProceso, SDC.twPuntoVenta, 
			case isnull(SSP.servidor,'') when '' then '' else QUOTENAME(SSP.servidor) + '.' end + quotename(SSP.base),
			SDC.phyCompro, SDC.phySucursal, SDC.phyTipoCompro, SSP.idPlanPrincipal
		From dbo.syncDespachosConfig SDC,
				dbo.syncServidoresPhysis SSP,
				dbo.[remitos resumen] RR
		where SDC.idUsuario = rr.nc_u -- RR.usuarioFaena   1/7/16. ver comentario arriba
			and RR.nc_rem = @TWNcRem
			and SDC.phyBase = ssp.id
			and sdc.twpuntoventa = case when rr.empresa='' and rr.sucursal='' then @CONSTPuntoVentaDefault
				when rr.empresa='' then rr.sucursal else rr.empresa end
	
	begin try
		begin tran
		if isnull((select count(1) from @BasesPhy),0) > 0	
		begin
			-- paso la fecha del remito a smalldatetime							
			Select @fecha = DATEADD(dd, 0, DATEDIFF(dd, 0, @TWFecha))
			open Proceso
			fetch next from Proceso into @Vproceso, @VtwPuntoVenta, @vphyBase, @VphyCompro, @VphySucursal, @VphyTipoCompro, @VIdPlanPpal
			while @@FETCH_STATUS = 0
			begin
				-- el cursor nos ayuda a recorrer el caminito que hizo el remito twins cuando se genero en physis (como remito, factura, etc)
				-- asi que la jugada ahora es buscar el comprobante donde deberia estar, y eliminarlo
				-- Obtengo el ejercicio (usaré el mismo para todos y todas)
				Select	@Sql = N'Select @PidEjercicio = (select idEjercicio ' +
								N'	from ' + @vphyBase + N'.dbo.ejercicios ' +
								N'	where @Phoy between fechaInicio and fechaCierre)'
				Select	@Param	=	N'@Phoy	smalldatetime, @PidEjercicio smallint OUTPUT '
				exec sp_executesql @sql, @param, @fecha, @PidEjercicio = @idEjercicio output
				if @idEjercicio is null
				begin
					Select @ErrorMessage = 'SDB. ERROR GRAVE. No se encontro el ejercicio contable definido para la fecha del comprobante NC_REM:' + cast(@TWNcRem as varchar)
					raiserror ( @ErrorMessage,16,1)
					if @@trancount > 0
						ROLLBACK tran 	
					return -1
				end
				if @modoDebug > 0
				Begin
					Print @sql
					Print 'SDB. Parametro de fecha utilizado: ' + cast(@fecha as varchar)
					Print 'SDB. Ejercicio detectado: ' + cast(isnull(@idEjercicio,'NULO') as varchar)
				End
				-- ahora busco el IdCabecera, la clave de los comprobantes:
				Select	@Sql = N'Select @PIdCabecera = (select top 1 IdCabecera ' +
								N'	from ' + @vphyBase + N'.dbo.FACCabeceras ' +
								N'Where	IdEjercicio = ''' + ltrim(rtrim(cast(@idEjercicio as varchar))) + ''' ' + 
								N'			and IdTipoComprobante	= ''' + ltrim(rtrim(cast(@VphyCompro as varchar))) + '''' + 
								N'			and IdPpal				= ''' + ltrim(rtrim(cast(@VIdPlanPpal as varchar))) + '''' +
								N'			and Numero = REPLICATE(''0'',12-(len(ltrim(rtrim(''' + cast(@TWRemitoNro as varchar) + '''))) + len(ltrim(rtrim('' ' + cast(@VphySucursal as varchar) + ' ''))) ))' + 
								N' + ltrim(rtrim('''+cast(@VphySucursal as varchar)+''')) + ltrim(rtrim('''+cast(@TWRemitoNro as varchar)+''')) ' +
								N'			and Sucursal			= ''' + ltrim(rtrim(cast(@VphySucursal as varchar))) + ''') ' 
				select @param = N'@PIdCabecera int out'
				exec sp_executesql @sql, @param, @PIdCabecera = @Idcabecera out
				if @modoDebug > 0
				begin
					Print @sql					
					print 'SDB. Cabecera detectado: ' + cast(isnull(@idcabecera,0) as varchar)
				end
				-- ahora hay que verificar si ese comprobante es de una base triple play y maneja alguna hija
				If (@VphyTipoCompro = 'P' or @VphyTipoCompro = 'F')
					Select @sql =	N'Select @PBaseHija = replace(baseRelacionada,''Siges'',''Sifac'')  From ' + @vphyBase +
							N'.dbo.TiposComprobante Where IdTipoComprobante like rtrim(ltrim(@PComp)) + ''%'' '
					else
						Select @sql =	N'Select @PBaseHija = replace(baseRelacionada,''Siges'',''Sifac'')  From ' + @vphyBase +
							N'.dbo.TiposComprobante Where IdTipoComprobante= @PComp'
				Select @param = N'@PComp varchar(5), @PBaseHija varchar(100) output'
				exec sp_executesql @sql, @param, @VphyCompro, @PBaseHija = @BaseHija output
				Select @BaseHija = left(@vphyBase,patindex('%.%',@vphyBase)) + quotename(@BaseHija)
				-- voy a revisar si los comprobantes de Physis que tendria que borrar estan siendo referenciados por otros
				Select @sql = N'Select @PCuenta = sum(A) from ( ' +
					N'Select count(1) A ' +
					N'from ' + @vphyBase + N'.dbo.Facstock ' +
					N'where idcabecera <> @PIdCabecera and remIdcabecera = @PIdCabecera ' +
					N'union ' +
					N'Select count(1) A ' +
					N'from ' + @vphyBase + N'.dbo.Facstock ' +
					N'where idcabecera <> @PIdCabecera and FacIdcabecera = @PIdCabecera ' +
					N'union ' +
					N'Select count(1) A ' +
					N'from ' + @vphyBase + N'.dbo.Facstock ' +
					N'where idcabecera <> @PIdCabecera and PedIdCabecera = @PIdCabecera	) AA' 
				Select @param = N'@PIdCabecera int, @PCuenta int output'
				if @modoDebug>0
					print @sql
				exec sp_executesql @sql, @param, @IdCabecera, @PCuenta = @Cuenta output
				if @modoDebug>0
					print 'SDB. Se hallaron ' + cast(@cuenta as varchar) + ' comprobantes relacionados'
				if @cuenta > 0
				begin
					Select @ErrorMessage = 'SDB. ¡Error! Hay comprobantes relacionados en Physis. NO SE PUDO ELIMINAR EN PHYSIS ; nc_rem: ' + cast(@TWNcRem as varchar)
					raiserror (@ErrorMessage,16,1)
				end
				SElect @Sql=NULL
				-- finalmente ejecuto la eliminacion
				-- si la opcion de syncInterfazGralTw llamada "EliminaAlAnular" esta en 1
				-- entonces va a eliminar el comprobante que se anule en twins
				-- caso contrario anula (default)
				
				if @modoDebug > 0
					print 'SDB. Se opta por ' + case when isnull(@elimino,'0')='0' then 'ANULAR' else 'ELIMINAR' end
				If isnull(@elimino,'0')='0'
					Select @sql = N'declare @fecha smalldatetime; select @fecha=getdate();exec ' + @vphyBase + '.dbo.spFACComprobantes_Anular '''
						 + ltrim(rtrim(cast(@IdCabecera as varchar))) + ''', @fecha'
				else 
					Select @sql =N'exec ' + @vphyBase + '.dbo.spFacComprobantes_Delete ''' + ltrim(rtrim(cast(@IdCabecera as varchar))) + ''''

				if @modoDebug > 0
					Print isnull(@sql,'NULO!')
				if @modoDebug < 2
					exec sp_executesql @sql
				if @baseHija is not null
				begin
					Select @sql = replace(@sql, @vphyBase, @BaseHija)
					if @modoDebug > 0
						Print @sql
					if @modoDebug < 2
						exec sp_executesql @sql--, @param, @IdConexion
				end
				if @modoDebug < 2
					Update	dbo.[REMITOS RESUMEN]
						Set		Obs			=	obs + '; comprobante eliminado en Physis '
						Where	NC_REM		=	@TWNcRem
				fetch next from Proceso into @Vproceso, @VtwPuntoVenta, @vphyBase, @VphyCompro, @VphySucursal, @VphyTipoCompro, @VIdPlanPpal	
			end
			close proceso
			deallocate proceso
			commit 
		end
		else
			Print 'SDB. No corresponde ninguna operacion de interfaz'
	end try
	begin catch
		print 'SDB. Error grave: '
		print ERROR_MESSAGE() 
		if @@trancount > 0
			ROLLBACK tran 	
		Select @ErrorMessage = 'SDB. Error al tratar de procesar eliminacion de comprobante por interfaz. ' + ERROR_MESSAGE() + '; nc_rem: ' + cast(@TWNcRem as varchar)
		raiserror (@ErrorMessage,16,1)
		Update	dbo.[REMITOS RESUMEN]
			Set		Obs			=	obs + '; imposible eliminar el comprobante en Physis'
			Where	NC_REM		=	@TWNcRem
	end catch





End

GO

