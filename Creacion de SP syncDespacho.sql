USE TwinsDb
GO
/****** Object:  StoredProcedure [dbo].[syncDespacho]    Script Date: 06/10/2014 14:10:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





IF not EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'syncDespacho')
   exec('CREATE Procedure dbo.syncDespacho  AS BEGIN SET NOCOUNT ON; END')
GO


alter  PROCEDURE [dbo].[syncDespacho] 
	@TWNcRem int = 0, 
	@modoDebug int = 0
AS
Begin
	set nocount on
	set xact_abort on

	-- Recibe por parametro el NC_REM, identificador unico de los remitos
	-- Por default en cero para catch-ear algun error de llamado.

	-- buscar si el nro ya existe para el comprobante y largar error
	-- partida la tengo definida como char(10) en algunos lugares de physis

/* Ejemplo de configuracion

--insert into TwinsDb.dbo.syncDespachosConfig
--	(idunico, idProceso, idUsuario, idSubUsuario, twbaseCodPro, twbasePeso, twbasePrecio, twPuntoVenta, phyBase,
--	phyCompro, phyDeposito, phySucursal, phyTipoCompro, movimientoNeg, phyprecio)
--select idunico+2, idProceso+2, idusuario, idsubusuario, twbasecodpro,twbasepeso, twbaseprecio, '0002', phybase, 
--	phyCompro, phyDeposito, '0002', phytipocompro, movimientoNeg, phyprecio
--from twinsdb.dbo.syncDespachosConfig

-- 8/12/13 
donde deje la marca SIN_SUBUSUARIO estan las condiciones (dos, tampoco tanto)
para validar si la mercaderia es de un subusuario (0 para propio, 8 en adelante para ajeno)
las deje comentadas en la release inicial

phyPrecio: 
0: Precio Twins 
1: Precio Physis

criterioPrecio:
0: No se pone precio;  (override del campo anterior)
1: pasar el precio "como esta"; 
2: agregar IVA al precio; 
3: sacar IVA al precio

cumplidoPropio
0: no se toca el "cumplido"
1: si el CUIT de vendedor y comprador son iguales, se da por cumplido el comprobante

*/
	set nocount on
	/*@ServerTwins
	Interfaz Twins - Physis

	Comprobante , numeradores
	CUIT generico
	Tropa generica
	Producto generico

	Pasaje de Remitos de Twins a Pedidos de Physis
	Usa tres SP de Physis, dos de ellos con bocha de parametros.
	Primero declaro las variables comunes, luego las que uso en el primero 
	y finalmente en el segundo SP (realmente el segundo y tercero).
	Luego inicializo las variables del primero y del segundo
	(Muchas de ellas quedan con valores por default)
	A continuacion "cargo" las variables que llevan valores particulares
	Despues genero una tabla en memoria con los datos del remito de Twins 
	que la empleo para cargar el pedido. 
	Sigue el uso de los tres SP: el primero para limpiar datos de la conexion (por cargas previas interrumpidas)
	el segundo para cargar el detalle (recorro la tabla en memoria con un cursor para eso)
	el tercero para dar el alta del comprobante, que a su vez relaciona el detalle previamente cargado.
	Al final boleteo la tabla temporal en memoria.

	Procedimiento general (version 2):
	- Leo la config general
	- Leo la ruta de los datos determinada por la parametrizacion de syncDespachosConfig
	- Acorde a eso armo la tabla variable con los datos que voy a usar

	La data que traigo de Twins:
	- Puede implicar cambio de codigo de producto, peso y precio
	O traigo todo en dos fuentes distintas
	O armo una vista en cada uno por nro de NC_REM y segun la necesidad leo una u otra


	Asumo del lado Physis:
	- mismo ejercicio para todos
	- mismo prefijo tropas twins para todos
	- mismo ancho de tropas para todos


	check list final:
	- quitar twinsdb como prefijo y dejar que asuma "la twinsdb de origen".
	- andara si hago SPs como "vistas" del SP original? de donde tomara las tablas?


	By Jair
	2010, 2012, 2013

	La condicion de pago, var @IdCondPago tendria que tomarla 
	del cliente (relacionada) salvo que no exista y tome por default una

	*/
	-- Configuracion particular de la implementacion:
	DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
	

	Declare @CONSTCriterioSufijoTropa		bit
	Declare	@CONSTPartidaNoEncontrada	varchar(20)
	Declare	@CONSTProductoNoEncontrado	varchar(20)
	Declare	@CONSTTerceroNoEncontrado	varchar(20)
	Declare	@CONSTPuntoVentaDefault		varchar(20)
	Declare @IdDeposito					char(5)  
	declare @IdCondPago					char(12)
	declare @IdTipoComprobante			char(8)			
	declare @Sucursal					Char(4)	
	Declare	@ConstIdAuxi				Smallint
	Declare	@ConstIdPpal				Smallint
	declare	@ConstIdAuxiCentroCostos	smallint
	declare @UM							char (5)                    
	declare @IdAuxi						smallint 
	Declare	@AnchoTropa smallint
	-- Variables del bucle de alta de comprobantes
	Declare @BaseHija	nvarchar(100)	
	Declare @AuxCuentaFilas				int
	declare	@ServPhy					nvarchar(100)
	Declare @MiCuit						varchar(12)

	-- leemos la configuracion de la nueva tabla - 16/7/15
	select top 1 @CONSTPartidaNoEncontrada = valor
	From	dbo.syncinterfazGralTw
	where opcion='idPdaNoEncontrada'

	select top 1 @CONSTProductoNoEncontrado = valor
	From	dbo.syncinterfazGralTw
	where opcion='idProdNoEncontrado'

	select top 1 @CONSTTerceroNoEncontrado = valor
	From	dbo.syncinterfazGralTw
	where opcion='idTerceroNoEncontrado'

	select top 1 @CONSTPuntoVentaDefault = valor
	From	dbo.syncinterfazGralTw
	where opcion='puntoVentaDefault'
	-- esta sintaxis rebuscada logra que se asigne un cero si el valor no existe en la tabla de parametrizacion
	-- caso contrario, si lo hago como los de arriba, queda en null
	select @CONSTCriterioSufijoTropa = isnull((select top 1 valor
	From	dbo.syncinterfazGralTw
	where opcion='sufijoTropa'),0)
	if @CONSTPartidaNoEncontrada is null or @CONSTProductoNoEncontrado is null or @CONSTTerceroNoEncontrado is null
	begin
		Select @ErrorMessage =  'Falta configurar en dbo.syncInterfazGralTw los codigos de excepcion de tercero, producto y/o partida'
		raiserror ( @ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end

	Select	@IdCondPago					= NULL		-- Por ahora hardcodeado
	Select	@ConstIdAuxi				= 1
	Select	@ConstIdPpal				= 1
	Select	@IdAuxi						= 1
	Select	@ConstIdAuxiCentroCostos	= 2
	if exists (select 1 from dbo.[REMITOS RESUMEN] 
				where NC_REM = @TWNcRem 
				and (ltrim(rtrim(remitonro)) ='' and @CONSTpuntoventadefault is null)
				)
	begin
		select @ErrorMessage = 'No es posible replicar un remito sin emitir (sin numero asignado) NC_REM:' + cast(@TWNcRem as varchar)
		raiserror (@errormessage ,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end
	if exists (select 1 from dbo.[REMITOS RESUMEN] where NC_REM = @TWNcRem and ltrim(rtrim(estado))='AN') 
	begin
		select @ErrorMessage = 'No es posible replicar un remito anulado NC_REM:' + cast(@TWNcRem as varchar)
		raiserror ( @ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end
	IF OBJECT_ID('tempdb..#cabecerasDevueltas') IS NOT NULL
	   DROP TABLE #CabecerasDevueltas

	Create table #CabecerasDevueltas	(
		NumeroDefinitivo varchar(20) null, cabecera int null, idcomprobante varchar(20) null)
 
	Declare	@sql	nvarchar(max)
	Declare	@param	nvarchar(max)
	Declare	@sql2	nvarchar(max)
	Declare	@param2	nvarchar(max)
	Declare @usu	int
	Declare @resul	int
	Declare	@resulVC	varchar(200)
	declare @sqlProvi nvarchar(max)
	-- Campos de parametrizacion general
	Declare @BasePhyDestino	nvarchar(100) -- especifica de la transaccion
	Declare @BasePhyReferencia	nvarchar(100) -- general de referencia phy
	Select	@BasePhyReferencia = (select top 1 
		case isnull(servidor,'') when '' then '' else QUOTENAME(servidor) + '.' end + quotename(base)
		from syncServidoresPhysis where habilitado='1' and principal='1')
	------------------------------------------------
	-- Empecemos por leer la config basica comun ---
	------------------------------------------------
	-- Esta es la lista "oficial" de bases de twins
	Declare @ServerTwins table
		(Numero int, NombreBase varchar(100));
	Select @sql = 'SELECT distinct id, base from ' + @BasePhyReferencia + '.dbo.syncServidoresTwins'
	Insert @ServerTwins
		exec (@sql)
	-- Notar que voy a la base Phy para leer esto, asi me aseguro de que sin importar de cual
	-- db de twins lo llamo, esto funcione.


	/*
	-- Si no se ha definido el prefijo, tomara el valor "1"
	Select	@sql = N'Select @PPrefijoTropasTwins = isnull((Select top 1 prefijoTropasTwins from ' +
		N'twinsdb.dbo.syncServidoresPhysis where habilitado=''1'' and principal=''1''),''1'')'
	exec sp_executesql @sql, N'@PPrefijoTropasTwins varchar(1) OUTPUT', @PPrefijoTropasTwins = @PrefijoTropasTwins OUTPUT
	If @modoDebug >0 
	begin
		Print 'Obtencion del prefijo de tropas:'
		Print @sql
		Print 'Prefijo obtenido: ' + @PrefijoTropasTwins
	End
	*/
	-- Determino el tamaño asignado a la tropa
	-- que corresponderia con el nivel mas bajo de la jerarquia de Partidas
	If @BasePhyReferencia is not null
	Begin
		Select	@Sql	= N'SELECT @PAnchoTropa = ( Select tamanio from ' + @BasePhyReferencia
			+ N'.dbo.FACNivelesPartidas Where IdNivelPartida = (Select  Max(IdNivelPartida) FROM ' + @BasePhyReferencia
			+ N'.dbo.FACNivelesPartidas Where Tamanio > 0 ) )'
		Select	@param	= N'@PAnchoTropa smallint OUTPUT'
		if @modoDebug > 0 and @modoDebug < 3
			Print @Sql
		exec sp_executesql @sql, @param, @PAnchoTropa = @AnchoTropa Output

		If @modoDebug > 0 and @modoDebug < 3
			Print 'Ancho de tropa calculado: ' + cast(@anchoTropa as varchar)
	end

	-- El deposito lo voy a tomar de la config para cada comprobante, no general
	---- La primera, busquemos el Deposito
	--Select	@Sql	=	N'SELECT	@PDeposito = IdaDeposito FROM ' +
	--					@BasePhyReferencia + N'.dbo.FACTropasImportacion_SetUp ' +
	--					N' WHERE	IdSoft = @PSoftIT'
	--Select	@Param	=	N'@PSoftIT		varchar(5), @PDeposito	varchar(12) output'
	--exec sp_executesql @sql, @Param, @SoftEsConsignatario, @PDeposito = @IdDeposito OUTPUT
	--If @modoDebug >0 
	--begin
	--	Print 'Obtencion del deposito:'
	--	Print @sql
	--	Print 'Deposito obtenido: ' + @IdDeposito
	--End


	declare @maxRemito 				int				-- aca guardo el remito mas reciente de Twins
	--declare @fecha 			smalldatetime 		-- la uso para ver la fecha del dia, y con eso el ejercicio
	-- Variables comunes a los dos SP:
	declare @idConexion				integer
	declare @cuit 					varchar(15)
	declare	@codigoCliente			varchar(10)		-- corresponde a la definicion de sCodigo en la tabla twinsdb.dbo.clientes
	-- Variables de SpFACStock_Tmp_Insert      
	declare @IdMovimiento			smallint 		/* En el alta va cero 0 */
	declare @NroOrden				numeric(4, 0)              /* Nro de Renglon */
	declare @Producto				char (20)                    /*Sale de la tabla FacProductos */
	declare @IdAuxiPropietario		smallint --                      Null
	declare @IdCtaAuxiPropietario	varchar(12)  --               Null
	declare @Partida				char (20) --                    Null /* o idpartida existente */
	declare @CantidadUM				numeric(13, 4)            /* Cantidad */
	declare @CantidadUMP			numeric(13, 4)            /* Cantidad en UMP */
	declare @PrecioUnitario			money 
	declare @Descuento				numeric(6, 3) 
	declare @PrecioUnitarioNeto		money  
	declare @PrecioNeto				money  
	declare @ImpuestosInternos		money  
	declare @FechaVencimiento		datetime 
	/* Fecha de vencimiento, en caso de der servicio sino es la misma del comprobante*/
	declare @ObservacionesSTI		varchar (2048) -- hay dos "observaciones"
	declare @AcumulaProducto		bit 
	declare @PedIdCabecera			int 
	declare @PedIdMovimiento		numeric(4,0) 
	declare @PedCantidad			numeric(13, 4) 
	declare @RemIdCabecera			int 
	declare @RemIdMovimiento		numeric(4,0)
	declare @RemCantidad			numeric(13, 4) 
	declare @FacIdCabecera			int 
	declare @FacIdMovimiento		numeric(4,0) 
	declare @FacCantidad			numeric(13,4) 
	declare @IdLiquidoProducto		int 
	declare @IdCabeceraViaje		int 
	declare @IdMovimientoViaje		numeric(4,0)
	declare @FacClase				Char(4) 
	declare @ProductoConjunto       char (12)
	declare @NivelConjunto          int 
	declare @IdPlanProducto         smallint 
	declare @RecuperoKgLimpio       money 

	declare @CantidadUMRemesa       numeric(13, 4)
	declare @CantidadUMDif          numeric(13, 4)
	declare @CantidadUMPorc         numeric(13, 4)
	declare @CantidadUMPRemesa     	numeric(13, 4)
	declare @CantidadUMPDif  		numeric(13, 4)
	declare @CantidadUMPPorc 		numeric(13, 4)
	declare @CodCampo 				int
	declare @CodLote 				int

	-- Variables de SpFACStock_Insert_Update_Ped

	declare @ABMD					char(1) /* ‘A‘o ‘M’ - Alta o Modificacion  en tu caso van a ser todas altas */             
	declare @IdCabecera				int                /* Al ser altas este parametro va en 0 */
	declare @IdEjercicio			smallint       /* En la tabla Ejercicios de Siges estan todos los ejercicios por rangos de fecha. */         

	declare @Fecha					datetime      /* Fecha Comprobante */     
	declare @Numero					varchar(12)  /* numero de Comprobante */         
	-- Esta es de SpFACStock_Insert_Update_Fac:
	declare	@TipoFactura			char(3)
	-- esto va para las facturas (o proformas)
	Declare	@TotalNeto				money = 0,
			@TotalIVA				money = 0,                       
			@TotalIVARNI			money = 0,                  
			@TotalPercepcionIVA		money = 0,                       
			@TotalFactura			money = 0,  
			@TotalNetoGravado       money = 0,          
			@TotalNetoNoGravado     money = 0
	/******* Datos del Tercero **********/
	declare @IdCtaAuxi				varchar(12)        
	declare @IdTipoDocumento		varchar(5)         
	declare @NumeroDocumento		varchar(12)         
	declare @NombreTercero			varchar(40)    /* Al nombre se lo saca de la tabla Cuentas Auxi*/
	declare @CategoriaIVA			varchar(2)         
	/**************************************/
	declare @ObservacionesSIUP		varchar(500)  -- esta es la segunda var de observaciones
	declare @IdAuxiListaPrecios		smallint
	declare @IdReagListaPrecios		smallint
	declare @IdListaPrecios			char(12)
	declare @IdReagVendedor			smallint
	declare @IdVendedor				char(12)
	declare @IdReagTransporte		smallint
	declare @IdTransporte			char(12)
	declare @IdReagDescuento		smallint
	declare @IdDescuento1			char(12)
	declare @Descuento1				money
	declare @IdDescuento2			char(12)
	declare @Descuento2				money
	declare @IdReagObservaciones	smallint
	declare @IdCodObservaciones		char(12)
	declare @Referencia				char(20)
	declare @IdReagCondPago			smallint
	--declare @ImporteTotal			money
	/***** Tipo de Comprobante *****/
	declare @Alcance				tinyint
	declare @ModoCarga				tinyint
	declare @IdMoneda				char(5)
	declare @Serie					tinyint
	declare @TasaCambio				float
	declare @GrabarViaje			bit
	declare @IdUsuario				smallint -- usar 0 = admin
	declare @forTranferWinsifac		Bit
	declare @IdCabeceraOUT			int
	declare @CodCampania			smallint
	declare @IdEstado				smallint
	declare @IdPais 				smallint
	declare @IdProvincia 			smallint
	declare @IdCabeceraRepl 		int		-- id de cabecera para replicacion consolidada-hijas
	declare	@IdComprobanteSigesRepl int		-- id de comprobante para relacion SIGES de factura/proforma
	-- Variables para el alta del remito
	declare @Planta					Bit
	declare	@FechaExt		    	datetime
    declare	@IdTipoComprobanteExt 	char(8)
    declare	@NumeroExt		    	varchar(12)
    declare	@FechaVencimientoCAI 	datetime
	declare	@FormaCosteo			char(5)
	-- las cargo con los valores por default, como para que no molesten
	Select	@Planta					= 1,
			@FormaCosteo			= '' 
/*			@FechaExt		    	= null,          
			@IdTipoComprobanteExt 	= null,          
			@NumeroExt		    	= null, 
			@FechaVencimientoCAI 	null */

	-- Cargo en cada parametro los valores por default
	-- Aunque podria dejar que los tome el SP directamente, no quiero arriesgarme
	-- a que en un cambio de version cambien dichos valores

	-- Esta es la estructura que voy a usar para reducir los ciclos de ejecucion
	Declare @DetallePhy table (
			DPNroOrden				smallint identity(1,1), -- no lo uso como nro de orden, pero si para ordenar los registros
			DPProducto				char(12),
			DPPartida				char(10),
			DPUM					char(5),
			DPCantidadUM			numeric(13,4),
			DPCantidadUMP			numeric(13,4),
			DPPrecioUnitario		money,
			DpPrecioUnitarioNeto	money, 
			DPPrecioNeto			money,
			ObservacionesSTI		varchar(2048)
	)

	Select @IdMovimiento				= Null /* En el alta va en 0 ¿? No andaba poniendo cero... */
	Select @PrecioUnitario				= 0
	Select @Descuento					= 0
	Select @PrecioUnitarioNeto			= 0          
	Select @PrecioNeto					= 0
	Select @ImpuestosInternos			= 0                                                         
	--Select @Observaciones = ''
	Select @AcumulaProducto				= 0
	Select @PedIdCabecera				= Null
	Select @PedIdMovimiento				= Null
	Select @PedCantidad					= Null
	Select @RemIdCabecera				= Null
	Select @RemIdMovimiento				= Null                             
	Select @RemCantidad					= Null
	Select @FacIdCabecera				= Null
	Select @FacIdMovimiento				= Null
	Select @FacCantidad					= Null
	Select @IdLiquidoProducto			= Null
	Select @IdCabeceraViaje				= Null
	Select @IdMovimientoViaje			= Null
	Select @ProductoConjunto			= Null
	Select @NivelConjunto				= Null
	Select @IdPlanProducto				= 1 /* IdPlanProducto de la Tabla FacProductos (siempre es 1) */  
	Select @RecuperoKgLimpio			= 0

	Select @CantidadUMRemesa			= 0 
	Select @CantidadUMDif				= 0 
	Select @CantidadUMPorc				= 0 
	Select @CantidadUMPRemesa			= NUll
	Select @CantidadUMPDif				= Null
	Select @CantidadUMPPorc				= Null
	Select @CodCampo					= Null
	Select @CodLote						= Null

	-- ==================================================================
	-- Campos de SpFACStock_Insert_Update_Ped
	/*
		Algunas explicaciones de los campos que no estoy poniendo aca
		(los completo mas adelante)
		@ABMD        char(1),  ‘A‘o ‘M’ - Alta o Modificacion  en este caso van a ser todas altas 
		@IdCabecera  int,                Al ser altas este parametro va en 0 
		@IdEjercicio smallint,        En la tabla Ejercicios de Siges estan todos los ejercicios por rangos de fecha.
		@Sucursal    Char(4),        0000 
		@Fecha       datetime,      Fecha Comprobante  
		@IdTipoComprobante  char(8),        IdComprobante de Pedido Valido 
		@Numero      varchar(12),   numero de Comprobante 
	 
		****** Datos del Tercero ********
		@IdAuxi      smallint,         Los datos estan en la tabla Terceros 
		@IdCtaAuxi   varchar(12),        
		@IdTipoDocumento varchar(5),         
		@NumeroDocumento varchar(12),         
		@NombreTercero   varchar(40),     Al nombre se lo saca de la tabla Cuentas Auxi
		@CategoriaIVA    varchar(2),         
	*/
	--Select @IdDeposito	= ''
	Select @IdAuxiListaPrecios			= null
	Select @IdReagListaPrecios			= null
	Select @IdListaPrecios				= null
	Select @IdReagTransporte			= null
	Select @IdTransporte				= null
	Select @IdReagDescuento				= null
	Select @IdDescuento1				= null
	Select @Descuento1					= 0
	Select @IdDescuento2				= null
	Select @Descuento2					= 0
	Select @IdReagObservaciones			= null
	Select @IdCodObservaciones			= null
	Select @Referencia					= ''
	--Select @IdReagCondPago				= 1 

	--Select @ImporteTotal				= 0
	Select @Alcance						= 3
	Select @ModoCarga					= 1
	Select @IdMoneda					= null
	Select @Serie						= null
	Select @TasaCambio					= 1
	Select @GrabarViaje					= 0
	Select @IdUsuario					= (select top 1 idoperador from syncServidoresPhysis where principal=1 and habilitado=1)
	Select @forTranferWinsifac			= 0
	Select @IdCabeceraOUT				= 0
	Select @CodCampania					= Null
	Select @IdEstado					= 1
	Select @IdPais						= 1 -- Argentina	
	Select @IdProvincia					= 2 -- Buenos Aires. No creo que sea indispensable
	Select @IdCabeceraRepl				= 0            
	Declare @varBase					int		 -- para el nro de base de datos segun la tabla de config

	-- Esto lo necesito para la movida consolidada:
	Declare @ComprobanteConso table (
		NumeroConsolidado varchar(12), 
		IdCabeceraConsolidado Int, 
		IdComproConsolidado Int
	)
	-- ==================================================================
	-- Ahora completo los campos que no van "por default"
	-- Campos comunes
	-- Voy a intentar generar un nro de conexion al azar, distinto para cada ejecucion
	-- sino mepa que esta interfiriendo una carga con otra cuando se generan los remitos
	-- en twins en simultaneo...
	-- esta es mi mejor aproximacion a un numero al azar
	Select @idConexion = DATEPART(ms, GETDATE()) * -1
	-- uso conexiones negativas, para diferenciarme de las conexiones positivas que son las de la GUI
	-- Esto no lo uso mas porque la fecha la saco del comprobante:
	--Select @fecha=cast(datepart(mm, getdate()) as nvarchar(2)) + '/' + cast(datepart(dd, getdate()) as nvarchar(2)) + '/' + cast(datepart(yyyy, getdate()) as nvarchar(4))
	-- Campos de SpFACStock_Tmp_Insert
	Select @IdAuxiPropietario			= Null
	Select @IdCtaAuxiPropietario		= Null
	Select @FechaVencimiento			= DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE()))
	--cast(datepart(mm, getdate()) as nvarchar(2)) + '/' + cast(datepart(dd, getdate()) as nvarchar(2)) + '/' + cast(datepart(yyyy, getdate()) as nvarchar(4))


	-- Campos de SpFACStock_Insert_Update_Ped
	Select @ABMD						= 'A'
	Select @idCabecera					= 0
	-----------------------------------------------------------------------------
	-- Llegado este punto tengo la data elemental.
	-- Ahora necesito diferenciar cada transaccion
	-----------------------------------------------------------------------------
	/*
	como puse mas arriba:
	- Leo la ruta de los datos determinada por la parametrizacion de syncDespachosConfig
	- Acorde a eso armo la tabla variable con los datos que voy a usar

	De la tabla syncDespachosConfig tengo que leer a partir de la info de origen.
	Lo unico que traigo de Twins es el NC_REM del despacho, a este punto ni siquiera 
	puedo determinar con certeza cual de las twinsDB's es la que invocó la interfaz

	*/

	-- No tengo forma de ver el "usuario encubierto" a menos que me fije codbar x codbar...
	-- Asi que primero me fijo eso (voy a determinar el usuario encubierto mayoritario)



	/*
	no estoy contemplando los remitos de productos sin codbar
	esos como los van a sacar? physis directo? twins?
	parametrizar por grupo? o por codbar=no?


	Arme un SP que devuelva los datos de UN NC_REM en cada base twinsdb
	Llamo a los SP y lleno una tabla variable
		Relaciono los tres datos: peso, producto, precio
	Un select final selecciona los tres segun la config

	*/


	Declare @tempRemitos table (
		[remitoTwins]		[varchar] (12)	NULL,
		[nroCarga]			[int]			NULL,
		[Codigo]			[varchar] (15)	NULL,
		[Descripcion]		[varchar] (100)	NULL,
		[Unidades]		[decimal](10,3)	NULL,
		[Peso]			[decimal](20,5)	NULL,
		[usuario]			int		NULL,
		[Tropa]				[nvarchar] (15)  NULL,
		[CodigoRemitoTwins] [int]			NULL,
		[CodigoPhysis]		[varchar] (15)	NULL,
		[CodigoInterno]		[int]			NULL,
		[cuit]				[varchar] (15)	NULL,
		[codigoCliente]		[varchar] (20)	NOT NULL,
		[usuarioSinMatricula] [int] not null,
		[precioUnit]		[decimal] (10,5) NULL,
		[precio]			[decimal] (15,5) NULL,
		[codbar]			[varchar]	(50)
	) 
	--Drop table ##tempRemitosBIS
	IF OBJECT_ID('tempdb..##tempRemitosBIS') IS NOT NULL
		DROP TABLE ##tempRemitosBIS
	Begin try
		Create table ##tempRemitosBIS (
			[remitoTwins]		[varchar] (12)	NULL,
			[nroCarga]			[int]			NULL,
			[Codigo]			[varchar] (15)	NULL,
			[Descripcion]		[varchar] (100)	NULL,
			[Unidades]			[decimal](10,3)	NULL,
			[Peso]				[decimal](20,5)	NULL,
			[usuario]			int				NULL,
			[Tropa]				[nvarchar] (15)  NULL,
			[CodigoRemitoTwins] [int]			NULL,
			[CodigoPhysis]		[varchar] (15)	NULL,
			[CodigoInterno]		[int]			NULL,
			[cuit]				[varchar] (15)	NULL,
			[codigoCliente]		[varchar] (20)	NOT NULL,
			[usuarioSinMatricula] [int] not null,
			[precioUnit]		[decimal] (10,5) NULL,
			[precio]			[decimal] (15,5) NULL,
			[codbar]			[varchar]	(50)
		) 
	end try
	begin catch
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Como ya estabas, te borro todo ##tempRemitosBIS'
		delete from ##tempRemitosBIS
	end catch
	insert @tempRemitos
		exec dbo.syncDespachoDetalle @TWNcRem
	-- Agrego una validacion: si no trajo ninguna fila, entonces ERRRRRROR y chau
	if (select count(1) from @tempRemitos)=0
	begin
		select @ErrorMessage =  'SD. Imposible procesar (no se encuentran) los datos para el remito NC_REM:' + cast(@TWNcRem as varchar)
		raiserror (@ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end

	Select	@codigoCliente = (Select top 1 codigoCliente from @tempRemitos)
	if @modoDebug > 0 and @modoDebug < 3
		Print 'Codigo de cliente a utilizar: ' + cast(isnull(@codigoCliente,'NULO') as varchar)
	/*	7/7/15
		el codigo de cliente que llega aqui puede ser:
		-	un codigo valido
		-	un codigo de cliente no encontrado
		-	un codigo no valido
	En teoria no deberia llegar un nulo o vacio o cosa asi.
	*/
	-- Al principio del SP ya validamos si el @CONSTTerceroNoEncontrado es valido
	-- asi que ahora parto de la premisa de que ese sirve
	-- 1) Me fijo si el codigo es valido y sino ya me quedo con el de NoEncontrado
	select @sql = N'Select @Pcliente=isnull((select IdCtaAuxi from ' + @BasePhyReferencia + 
		'.dbo.Terceros where idCtaauxi=' + ltrim(rtrim(cast(@codigoCliente as varchar))) + '),' 
		+ ltrim(rtrim(cast(@CONSTTerceroNoEncontrado as varchar))) + ')'
	Select @param = N'@Pcliente varchar(20) output'
	exec sp_executesql @sql, @param, @Pcliente = @codigoCliente output
	if @modoDebug > 0 and @modoDebug < 3
		print isnull(@sql,'Busqueda de cliente dio una cadena SQL nula')
	-- 2) Si el que trajo es el de no encontrado, pruebo buscar por CUIT
	if @codigoCliente = @CONSTTerceroNoEncontrado collate Modern_Spanish_CI_AS
	begin
		-- Trato de ubicarlo por CUIT, ya que no esta el codigo de cliente en twins
		-- si no esta por CUIT, queda el codigo que estaba (el de "no encontrado")
		if @modoDebug > 0 and @modoDebug < 3
			Print 'El codigo de cliente no estaba cargado en Twins (cod_adm_cli) asi que intento ubicarlo por cuit'
		select @sql = N'Select @PIdCtaAuxi=isnull(IdCtaAuxi,'+ltrim(rtrim(cast(@codigoCliente as varchar)))+') from ' + @BasePhyReferencia + '.dbo.Terceros where NumeroDocumento=''' + 
			(select top 1 ltrim(rtrim(cast(replace(cuit,'-','') as varchar))) from @tempRemitos) + ''' collate Modern_Spanish_CI_AS'
		Select @param = N'@PIdCtaAuxi varchar(20) output'
		exec sp_executesql @sql, @param, @PIdCtaAuxi = @codigoCliente output
		if @modoDebug > 0 and @modoDebug < 3
		begin
			print isnull(@sql,'Busqueda de cliente por cuit dio una cadena SQL nula')
			print @codigocliente
		end
	end
	if @codigoCliente is null
	begin
		select @ErrorMessage =  'Error desconocido al tratar de identificar el cliente del remito con NC_REM:' + cast(@TWNcRem as varchar)
		raiserror (@ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end
	Select	top 1 @usu = usuarioSinMatricula
	From	@tempRemitos
	group by usuarioSinMatricula
	Order by count(codbar) desc -- el mas aparecido primero
	if @modoDebug > 0 and @modoDebug < 3
		print 'Usuario (sin matricula) detectado: ' + isnull(cast(@usu as varchar),'NULO')
	
	-- El remito emitido en twins tiene mercaderia que puede ser de mas de un usuario
	-- en realidad las TROPAS pueden ser de mas de un usuario, pero la mercaderia deberia
	-- pertenecer al que lo esta llevando. (El de [remito resumen].usuario, justo de ahi lo saco)
	Declare	@UsuarioPrincipalRemito int
	
	Select @UsuarioPrincipalRemito = (	Select usuario from (select top 1 usuario, COUNT(codbar) cuenta
														from @tempRemitos
														GROUP by usuario
														order by cuenta desc) A  )
	if @modoDebug > 0 and @modoDebug < 3
		Print 'Usuario principal detectado: ' + cast (	isnull(@UsuarioPrincipalRemito,'-1') as varchar)											


	-- Esta tabla temporal la uso a mitad de trabajo...
	Begin try
		Create table ##productosBuscados (
			PBIdProducto		char(12),
			PBpreciounitario	money,
			PBunidadMedida		char(5)
		)
	end try
	begin catch
		delete from ##productosBuscados
	end catch
	-- Ya tengo todo para leer la parametrizacion
	-- 16/7/15. Agrego manejo de punto de venta default. 
	-- Si el punto de venta viene vacio (puede ser el campo sucursal o empresa)
	-- entonces tiene que haber parametrizacion para el punto de venta default
	if isnull(	(Select count(1)
			From dbo.syncDespachosConfig SDC, dbo.[remitos resumen] RR
			where SDC.idUsuario = RR.nc_u -- SIN_SUBUSUARIO and SDC.idSubUsuario=@usu -- PROBLEMOTE. Este dato no lo tengo tan facil.
				and (SDC.twPuntoVenta=RR.empresa collate Modern_Spanish_CI_AS 
				or SDC.twPuntoVenta=RR.sucursal collate Modern_Spanish_CI_AS 
				or ((rr.empresa = '' and @CONSTPuntoVentaDefault = SDC.twPuntoVenta) 
					or (rr.sucursal = '' and @CONSTPuntoVentaDefault = SDC.twPuntoVenta) )
				) and RR.nc_rem = @TWNcRem
		),0) < 1
	begin
		-- Si no tengo el "caminito" del remito, hasta aqui llegó mi amor
		select @ErrorMessage =  'Imposible hallar parametrizacion para procesar el remito NC_REM:' + cast(@TWNcRem as varchar)
		raiserror (@ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end	
	-- 30/12/2015 Antes de seguir, reviso si el comprobante existe en Physis. Si es asi, genero un error
	-- verifico Tipo y nro de comprobante, pero no la fecha, OJO! o sea que con distinta fecha da error (lo cual es deseable)
	-- Debo contemplar que un comprobante twins genere mas de un comprobante physis
	-- Usando las estructuras de parametrizacion, busco en las bases physis si el comprobante -que debiera no existir aun- ya existe
	declare @control tinyint;
	Select @sql = null
	Select @sql = coalesce(@sql + N' union Select 1 AA from ' + case isnull(SSP.servidor,'') when '' then '' else QUOTENAME(SSP.servidor) + '.' end + quotename(SSP.base)
		+ '.dbo.FacCabeceras Where Numero=''' + cast(SDC.phySucursal as varchar) 
			+ case when cast(rr.remitonro as varchar) = '' 
			then right('00000000' + cast(rr.nc_rem as varchar),8)
			else cast(rr.remitonro as varchar) end + ''' and IdTipoComprobante=''' + ltrim(rtrim(SDC.phyCompro)) + ''''
		,N' Select 1 AA from ' + case isnull(SSP.servidor,'') when '' then '' else QUOTENAME(SSP.servidor) + '.' end + quotename(SSP.base)
		+ '.dbo.FacCabeceras Where Numero=''' + cast(SDC.phySucursal as varchar) 
			+ case when cast(rr.remitonro as varchar) = '' 
			then right('00000000' + cast(rr.nc_rem as varchar),8)
			else cast(rr.remitonro as varchar) end + ''' and IdTipoComprobante=''' + ltrim(rtrim(SDC.phyCompro)) + '''')
		From dbo.syncDespachosConfig SDC, dbo.[remitos resumen] RR, dbo.syncServidoresPhysis SSP
		where SDC.idUsuario = RR.nc_u  -- SIN_SUBUSUARIO and SDC.idSubUsuario=@usu -- PROBLEMOTE. Este dato no lo tengo tan facil.
			and RR.nc_rem = @TWNcRem
			and SDC.phyBase = SSP.id
			and ((SDC.twPuntoVenta=RR.empresa collate Modern_Spanish_CI_AS or SDC.twPuntoVenta=RR.sucursal collate Modern_Spanish_CI_AS )
				or (rr.remitonro = '' and @CONSTPuntoVentaDefault = SDC.twPuntoVenta) )
	Select @sql = 'set @bandera=0; if exists (' + @sql + ') set @bandera=1'
	Select @param = N'@bandera tinyint output'
	if @modoDebug > 0
		Print 'SD.' + isnull(@sql,'NULOOO')
	exec sp_executesql @sql, @param, @bandera = @control output;
	if isnull(@control ,0) = 1
	begin
		Select @errormessage = 'El comprobante que intenta replicar ya existe en Physis'
		raiserror (@errormessage,16,1);
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end
	-- con esto mantengo limpita la tabla temporal intermedia de insercion en facStockAuxiliares
	declare LimpioCursor cursor local for
		Select distinct case isnull(servidor,'') when '' then '' else QUOTENAME(servidor) + '.' end + quotename(base)
		from syncServidoresPhysis where habilitado='1'
	Open LimpioCursor
	Fetch next from limpiocursor into @ServPhy
	While @@fetch_status = 0
	begin
		Select @sql = 'Delete from ' + @servphy + '.dbo.FACStockAuxiliares_Tmp where conexion=''' + ltrim(rtrim(cast(@idconexion as varchar)))+ ''''
		exec sp_executesql @sql
		if @modoDebug > 0
			print @sql
		Fetch next from limpiocursor into @ServPhy
	end
	close LimpioCursor
	Deallocate LimpioCursor

	Begin Transaction altaRemitos
	-- agrego manejo para salida de despachos
	-- contemplando que no haya nro de remito (para no preguntar por sucursal y empresa) 
	-- pero que SI haya parametrizacion para el punto de venta default 16/7/15
	Declare MasterCursor cursor local For
		Select SDC.idProceso, SDC.twbaseCodPro, SDC.twbasePeso, SDC.twBasePrecio, SDC.phyPrecio, SDC.criterioPrecio, 
			SDC.phyBase, SDC.phyCompro, SDC.phyDeposito, SDC.phySucursal, SDC.phyTipoCompro, SDC.movimientoNeg, SDC.procesaCCajena, SDC.phyFechaTw
			,SDC.cumplidoPropio
		From dbo.syncDespachosConfig SDC, dbo.[remitos resumen] RR
		where SDC.idUsuario = RR.nc_u  -- SIN_SUBUSUARIO and SDC.idSubUsuario=@usu -- PROBLEMOTE. Este dato no lo tengo tan facil.
			and RR.nc_rem = @TWNcRem
			and ((SDC.twPuntoVenta=RR.empresa collate Modern_Spanish_CI_AS or SDC.twPuntoVenta=RR.sucursal collate Modern_Spanish_CI_AS )
				or (rr.remitonro = '' and @CONSTPuntoVentaDefault = SDC.twPuntoVenta) )
		Order by sdc.idproceso 
	Open MasterCursor 
	Declare @MCidProceso		int,
			@MCtwBaseCodPro		int,
			@MCtwBasePeso		int,
			@MCtwBasePrecio		int,
			@MCphyPrecio		int,
			@MCcriterioPrecio	int,
			@MCphyBase			int,
			@MCphyCompro		char(8),
			@MCphyDeposito		char(5),
			@MCSucursal			char(4),
			@MCphyTipoCompro	char(1),
			@MCmovimientoNeg	bit,
			@MCprocesaCCajena	int,
			@MCphyFechaTw		smallint,
			@MCcumplidoPropio	bit
	Declare
			@ANTMCidProceso		int,
			@ANTMCphyCompro		char(8),
			@ANTMCphyDeposito	char(5),
			@ANTMCSucursal		char(4),
			@ANTMCphyTipoCompro	char(1),	-- todas para el cierre de un comprobante en el corte de control
			@ANTMCprocesaCCajena	int,
			@ANTMCphyPrecio		int,
			@ANTMCphyFechaTw	smallint,
			@ANTMCcumplidoPropio bit

	Fetch Next from MasterCursor into @MCidProceso,	@MCtwBaseCodPro, @MCtwBasePeso,	@MCtwBasePrecio, @MCphyPrecio, @MCcriterioPrecio,
			@MCphyBase,	@MCphyCompro, @MCphyDeposito, @MCSucursal, @MCphyTipoCompro, @MCmovimientoNeg, @MCprocesaCCajena, @MCphyFechaTw
			,@MCcumplidoPropio
	While @@Fetch_status = 0
	Begin
		if @modoDebug > 0 and @modoDebug < 3
		Begin
			Print '********Proceso: ' + cast(@MCidProceso as varchar) + ' twBaseCodPro: ' + cast(@MCtwBaseCodPro as varchar)
			+ ' twBasePeso: ' + cast(@MCtwBasePeso as varchar) + ' twBasePrecio: ' + cast(@MCtwBasePrecio as varchar)
			+ ' phyPrecio: ' + case @MCphyPrecio when 0 then 'de Twins' when 1 then 'de Physis' end
			+ ' criterio precio: ' + case @MCcriterioPrecio when 0 then 'SIN PRECIO!' when 1 then 'sin tocar' when 2 then 'agregarle IVA' when 3 then 'sacarle IVA' when 4 then 'No se!' end
			+ ' phyBase: ' + cast(@MCphyBase as varchar) + ' phyCompro: ' + cast(@MCphyCompro as varchar)
			+ ' phyDeposito: ' + cast(@MCphyDeposito as varchar) + ' Sucursal: ' + cast(@MCSucursal as varchar)
			+ ' phyTipoCompro: ' + cast(@MCphyTipoCompro as varchar) + ' movimientoNeg: ' + case when @MCmovimientoNeg=0 then 'NO' else 'SI' end
			+ ' proceso CC ajenos? ' + case @MCprocesaCCajena when 0 then 'No' else 'Si' end
			+ ' uso la fecha de twins? ' + case @MCphyFechaTw when 0 then 'No' else 'Si' end
			+ ' doy por cumplido si es propio? ' + case @MCcumplidoPropio when 0 then 'No' else 'Si' end
			Print 'Procesos 1:'
			Print @MCidproceso
			Print 'Procesos 2:'
			if @Antmcidproceso is null
				Print 'Aun no hay proceso anterior determinado'
			else
				Print @Antmcidproceso
		End
		If (select count(1) from @DetallePhy) > 0 -- significa que traemos data de un ciclo previo
			and @MCIdProceso <> isnull(@ANTMCidProceso,9999) -- o sea que es un proceso distinto
		Begin
			-- tengo que darle el alta antes de seguir con otro comprobante...
			/* Finalmente, a los bollos
			- borro lo que haya dando vueltas (no deberia ser nada, peeeero...)
			- cargo los renglones (detalle) del pedido
			- cargo el pedido (encabezado)
			*/
			if @ANTMCphyFechaTw	= 1 -- significa que la fecha que vale es la del comprobante en Twins
			begin
				Select	@fecha = convert(smalldatetime,fecha) -- (	Select	cast(left(fecha,4) as nvarchar(4)) + '/' + 
							--CAST(LEFT(RIGHT(fecha,4),2) as nvarchar(2)) + '/' +
							--cast(right(fecha,2) as nvarchar(2))
				from dbo.[remitos resumen] 
				where nc_rem = (select top 1 CodigoRemitoTwins from @tempRemitos)
				--Select @fecha = DATEADD(hh,cast(left(hora,2) as int),DATEADD(MINUTE,cast(LEFT(RIGHT(hora,5),2) as int),@fecha))
				--							from dbo.[remitos resumen] 
				--					where nc_rem = (select top 1 CodigoRemitoTwins from @tempRemitos)
				if @modoDebug > 0 and @modoDebug < 3
					Print 'Fecha y hora del comprobante: ' + cast(isnull(@fecha,'NULO') as varchar)
			end
			else -- sino, me quedo con solo la fecha
				Select @fecha = DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE()))
			-- Obtengo el ejercicio (usaré el mismo para todos y todas)
			-- Tomo el ejercicio de la fecha de ejecucion de la importacion
			Select	@Sql = N'Select @PidEjercicio = (select idEjercicio ' +
							N'	from ' + @BasePhyReferencia + N'.dbo.ejercicios ' +
							N'	where @Phoy between fechaInicio and fechaCierre)'
			Select	@Param	=	N'@Phoy	smalldatetime, @PidEjercicio smallint OUTPUT '
			exec sp_executesql @sql, @param, @fecha, @PidEjercicio = @idEjercicio output
			if @idEjercicio is null
			begin
				Select @ErrorMessage = 'No hay ningun ejercicio contable definido para la fecha del comprobante NC_REM:' + cast(@twncrem as varchar)
				raiserror ( @ErrorMessage,16,1)
				if @@trancount > 0
					ROLLBACK tran 	
				return -1
			end
			if @modoDebug > 0 and @modoDebug < 3
			Begin
				Print @sql
				Print 'Parametro de fecha utilizado: ' + cast(@fecha as varchar)
				Print 'Ejercicio detectado: ' + cast(isnull(@idEjercicio,'NULO') as varchar)
			End
			-- voy a verificar que el comprobante exista (con su numerador) en Physis
			-- y si no esta, chau!
			--Select @sql = N'Select @PResul=1 from ' + @BasePhyDestino + '.dbo.TiposComprobante TC inner join ' +
			--@BasePhyDestino + '.dbo.NumeradoresPrefijos NP on NP.IdNumerador=TC.IdNumerador ' +
			--' where  TC.idtipocomprobante = ''' + ltrim(rtrim(@MCphyCompro)) + ''' collate  Modern_Spanish_CI_AS ' +
			--' and (replicate(''0'', 4 - len(NP.IdPrefijo)) + cast (NP.IdPrefijo as varchar)) = ''' + ltrim(rtrim(cast(@MCSucursal as varchar))) 
			--+ ''' collate  Modern_Spanish_CI_AS '
			--if @modoDebug < 2
			--	exec sp_executeSql @sql, N'@Presul int output', @Presul = @resul output
			--if @modoDebug > 0
			--	print @sql
			--if isnull(@resul,0)=0 -- significa que el comprobante no existe con ese numerador
			--begin
			--	select @ErrorMessage='El comprobante ' + @mcphycompro + ' con el punto de venta ' + @mcsucursal + ' no esta definido en Sifac'
			--	raiserror(@errormessage,16,1)
			--	if @@trancount > 0
			--		rollback tran
			--	return -1
			--end
			-- 1ro borramos lo que haya para la conexion
			Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.spFACStock_Tmp_Delete ''' + cast(@IdConexion as nvarchar) + ''''
			--Select	@param = N'@PidConexion smallint'
			if @modoDebug < 2
			begin try
				exec sp_executesql @sql--, @param, @IdConexion
			end try
			begin catch
				print 'Error grave: '
				print ERROR_NUMBER() 
				print ERROR_SEVERITY() 
				print ERROR_STATE() 
				print ERROR_PROCEDURE() 
				print ERROR_LINE() 
				print ERROR_MESSAGE() 
				if @@trancount > 0
					ROLLBACK tran 	
				Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
				raiserror (@ErrorMessage,16,1)
				return -1
			end catch
			if @modoDebug > 0
				print @sql
			-- y de la hija!
			Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
			if @baseHija is not null
			begin
				Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
				if @modoDebug < 2
					begin try
						exec sp_executesql @sql--, @param, @IdConexion
					end try
					begin catch
						print 'Error grave: '
						print ERROR_NUMBER() 
						print ERROR_SEVERITY() 
						print ERROR_STATE() 
						print ERROR_PROCEDURE() 
						print ERROR_LINE() 
						print ERROR_MESSAGE() 
						if @@trancount > 0
							ROLLBACK tran 	
						Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
						raiserror (@ErrorMessage,16,1)
						return -1
					end catch
				if @modoDebug > 0
					print @sql
			end
			if @modoDebug > 0 and @modoDebug < 3
				print 'Base 2:' + isnull(@sql,'NULA')
			-- Uso un CURSOR para recorrer la tabla temporal creada y dar todo de alta
			Select @NroOrden = 0 -- inicio el renglon en cero
			Select @CantidadUM = (select count(*) from @detallephy)
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Cantidad de filas a procesar: ' + cast(isnull(@cantidadUM,'NULA') as varchar)

			DECLARE remitoCur cursor local For 
				SELECT 	DPProducto, case DPPartida when 0 then null else DPPartida end,DPUM,DPCantidadUM,DPCantidadUMP,
						DPPrecioUnitario,DpPrecioUnitarioNeto,DPPrecioNeto,ObservacionesSTI		
				FROM 	@DetallePhy
			OPEN remitoCur
			FETCH NEXT FROM remitoCur
				INTO @Producto, @Partida, @UM, @CantidadUM, @CantidadUMP, @precioUnitario,
						@PrecioUnitarioNeto,@precioNeto, @ObservacionesSTI
			WHILE @@FETCH_STATUS = 0
			BEGIN
				--Esto me dijo Fabian San Martin en mail del 16/6/14. ay caramba!
				--Cuando estas grabando los remitos de entrada estas poniendo 0 en FacIdCabecera, FacIdMovimiento y FacCantidad, cuando deberias grabar NULL.
				--y el 31/10 me recordo que las columnas FacIdCabecera, FacIdMovimiento y FacCantidad de los PR
				--deberian contar con el mismo valor que IdCabecera, IdMovimiento, FacCantidad
				Select @FacIdCabecera=NULL,@FacIdMovimiento=NULL
				if (@ANTMCphyTipoCompro = 'P' or @ANTMCphyTipoCompro='F')
					Select @FacClase = 'REM',
						@TipoFactura = 'REM'
				else
					Select @FacClase = '',
						@TipoFactura = ''
				if @ANTMCphyTipoCompro='R'
					Select @FacCantidad=NULL
				else
					Select @FacCantidad= case when @CantidadUMP=0 then @cantidadUM else @cantidadUMP end
				Select @NroOrden = @NroOrden + 1 -- incremento el renglon (empieza en cero)
				/********* Ahora voy a leer la unidad de stock del producto en cuestion para utilizarla *****/
				if @partida='0'
					Select @partida = Null
				-- tengo que dejarla NULA para que no la cargue cuando se trata de cajas
				-- aunque hasta este punto considere como '0' las cajas
				-- porque NULLs quedaron las tropas no encontradas (de colgado), que tuve que reemplazar por la generica
				-- ahora si, vamo' pa' delante...
				if @UM is null
				begin
					Print 'SD. ERROR GRAVE. La unidad de medida (UM) del producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definida'
					if @@trancount > 0
						rollback tran
					Select @ErrorMessage = 'SD. ERROR GRAVE. La unidad de medida (UM) del producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definida'
					raiserror (@ErrorMessage,16,1)
					return -1
				end
				if @Producto is null
				begin
					Print 'SD. ERROR GRAVE. El codigo de producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definido'
					if @@trancount > 0
						rollback tran
					Select @ErrorMessage =  'SD. ERROR GRAVE. El codigo de producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definido'
					raiserror (@ErrorMessage,16,1)
					return -1
				end
				Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.SpFACStock_Tmp_Insert ' +
					ISNULL('''' + cast(@IdMovimiento as varchar) + '''',' NULL ') + ', ' +								
					'''' + cast(@NroOrden as varchar) + '''' + ', ' +
					'''' + ltrim(rtrim(cast(@Producto as varchar))) + '''' + ', ' +
					ISNULL('''' + cast(@IdAuxiPropietario as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@IdCtaAuxiPropietario as varchar) + '''',' NULL ') + ', ' +												 
					ISNULL('''' + ltrim(rtrim(cast(@Partida as varchar))) + '''',' NULL ') + ', ' +								
					'''' + cast(@UM as varchar) + '''' + ', ' +
					'''' + cast(@CantidadUM as varchar) + '''' + ', ' +
					'''' + cast(@CantidadUMP as varchar) + '''' + ', ' +
					quotename(isnull(@PrecioUnitario,0))  + ', ' +
					quotename(isnull(@Descuento ,0))  + ', ' +
					quotename(isnull(@PrecioUnitarioNeto,0))  + ', ' + -- preciounitario
					quotename(isnull(@PrecioNeto,0))  + ', ' +	-- cantidadUMP (o cantidad) * precioneto
					quotename(isnull(@ImpuestosInternos,0)) + ','+
					'''' + cast(@FechaVencimiento as varchar) + '''' + ', ' +
					'''' + @ObservacionesSTI + '''' + ', ' +
					'''' + cast(@AcumulaProducto as varchar) + '''' + ', ' +
					ISNULL('''' + cast(@PedIdCabecera as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@PedIdMovimiento as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@PedCantidad as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@RemIdCabecera as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@RemIdMovimiento as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@RemCantidad as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@FacIdCabecera as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@FacIdMovimiento as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@FacCantidad as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@IdLiquidoProducto as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@IdCabeceraViaje as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@IdMovimientoViaje as varchar) + '''',' NULL ') + ', ' +								
					'''' + cast(@IdConexion as varchar) + '''' + ', ' +
					'''' + @FacClase  + '''' + ', ' +
					ISNULL('''' + cast(@ProductoConjunto as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@NivelConjunto as varchar) + '''',' NULL ') + ', ' +								
					'''' + cast(@IdPlanProducto as varchar) + '''' + ', ' +
					'''' + cast(@RecuperoKgLimpio as varchar) + '''' + ', ' +
					'''' + ltrim(rtrim(@ANTMCphyDeposito)) + '''' + ', ' +
					'''' + cast(@CantidadUMRemesa as varchar) + '''' + ', ' +
					'''' + cast(@CantidadUMDif as varchar) + '''' + ', ' +
					'''' + cast(@CantidadUMPorc as varchar) + '''' + ', ' +
					ISNULL('''' + cast(@CantidadUMPRemesa as varchar) + '''',' NULL ') + ', ' +
					ISNULL('''' + cast(@CantidadUMPDif as varchar) + '''',' NULL ') + ', ' +
					ISNULL('''' + cast(@CantidadUMPPorc as varchar) + '''',' NULL ') + ', ' +
					ISNULL('''' + cast(@CodCampo as varchar) + '''',' NULL ') + ', ' +								
					ISNULL('''' + cast(@CodLote as varchar) + '''',' NULL ') 
				if @modoDebug > 0 and @modoDebug < 3
					print 'Alta de renglon de detalle en comprobante (UNO)'
				if @modoDebug > 0
					Print @sql
				if @modoDebug < 2
				begin try
					exec sp_executesql @sql--, @param, @IdConexion
				end try
				begin catch
					print 'Error grave (Stock - conso 1): '
					print ERROR_NUMBER() 
					print ERROR_SEVERITY() 
					print ERROR_STATE() 
					print ERROR_PROCEDURE() 
					print ERROR_LINE() 
					print ERROR_MESSAGE() 
					if @@trancount > 0
						ROLLBACK tran 	
					Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
					close remitoCur
					deallocate remitoCur
					raiserror (@ErrorMessage,16,1)
					return -1
				end catch
				If @BaseHija is not null
				begin
					Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
					if @modoDebug > 0 and @modoDebug < 3
						Print 'Hija: ' + isnull(@sql,'NULO') -- el de la hija
					if @modoDebug > 0
						print @sql
					if @modoDebug < 2
						begin try
							exec sp_executesql @sql--, @param, @IdConexion
						end try
						begin catch
							print 'Error grave (Stock - hija 1): '
							print ERROR_NUMBER() 
							print ERROR_SEVERITY() 
							print ERROR_STATE() 
							print ERROR_PROCEDURE() 
							print ERROR_LINE() 
							print ERROR_MESSAGE() 
							if @@trancount > 0
								ROLLBACK tran 	
							Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
							raiserror (@ErrorMessage,16,1)
							return -1
						end catch
				end
				else
					if @modoDebug > 0
						Print isnull(@BaseHija,'Base hija NULA')
				-- Ahora el alta para el centro de costos (29-09-2014)
				if (@ANTMCphyTipoCompro = 'F' or @ANTMCphyTipoCompro='P') and isnull(@partida,'0') <> '0' and 1=0
				begin
					-- Verifico si ya lo habia cargado, si hay mas productos de la misma partida/tropa, lo estar duplicando
					Select @sql='Select @PResul=count(1) from ' + @BasePhyDestino + N'.dbo.FACStockAuxiliares_Tmp where Idctaauxiliar = ''' + ltrim(rtrim(cast(@Partida as varchar))) +
						''' and conexion = ''' + ltrim(rtrim(cast(@IdConexion as varchar))) + ''' and idplanauxiliar= ''' + ltrim(rtrim(cast(@ConstIdAuxiCentroCostos as varchar))) + ''''
					Select @Param = '@PResul int output'
					exec sp_executesql @sql, @param, @Presul = @resul output
					if isnull(@resul,0)=0
					begin
						Select @IdMovimiento = 0 --(Solo tiene si estoy editando en el alta van en 0)
						Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.spFACStockAuxiliaresTMP_Insert ' +
						ISNULL('''' + cast(@IdCabecera as varchar) + '''',' NULL ') + ', ' +								
						ISNULL('''' + cast(@IdMovimiento as varchar) + '''',' NULL ') + ', ' +								
						'''' + cast(@NroOrden as varchar) + '''' + ', ' +
						'''' + cast(@ConstIdAuxiCentroCostos as varchar) + '''' + ', ' +
						ISNULL('''' + ltrim(rtrim(cast(@Partida as varchar))) + '''',' NULL ') + ', ' +	--@IdCtaAuxiliar Numero de cuenta (en este caso es el mismo Nro de la Tropa)
						'''' + cast(@IdConexion as varchar) + '''' 
						/*
						spFACStockAuxiliaresTMP_Insert     (@IdCabecera int, @IdMovimiento smallint, @NroOrden Tinyint, 
							 @IdPlanAuxiliar smallint, @IdCtaAuxiliar varchar(12), @Conexion int)

						donde :
						@IdCabecera, @IdMovimiento (Solo tiene si estoy editando en el alta van en 0)
						@NroOrden  en la grilla. 
						@IdPlanAuxiliar Id del plan de Centros de Costo
						@IdCtaAuxiliar Numero de cuenta (en este caso es el mismo Nro de la Tropa)
						@Conexion 
						*/
						if @modoDebug > 0 and @modoDebug < 3
							print 'Alta de renglon de detalle para centro de costos (UNO)'
						if @modoDebug > 0
							Print @sql
						if @modoDebug < 2
						begin try
							exec sp_executesql @sql--, @param, @IdConexion
						end try
						begin catch
							print 'Error grave: '
							print ERROR_NUMBER() 
							print ERROR_SEVERITY() 
							print ERROR_STATE() 
							print ERROR_PROCEDURE() 
							print ERROR_LINE() 
							print ERROR_MESSAGE() 
							if @@trancount > 0
								ROLLBACK tran 	
							Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
							close remitoCur
							deallocate remitocur
							raiserror (@ErrorMessage,16,1)
							return -1
						end catch
						If @BaseHija is not null
						begin
							Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
							if @modoDebug > 0
								print @sql
							begin try
								exec sp_executesql @sql--, @param, @IdConexion
							end try
							begin catch
								print 'Error grave: '
								print ERROR_NUMBER() 
								print ERROR_SEVERITY() 
								print ERROR_STATE() 
								print ERROR_PROCEDURE() 
								print ERROR_LINE() 
								print ERROR_MESSAGE() 
								if @@trancount > 0
									ROLLBACK tran 	
								Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
								raiserror (@ErrorMessage,16,1)
								return -1
							end catch
						end
					end
				end
				FETCH NEXT FROM remitoCur
					INTO @Producto, @Partida, @UM, @CantidadUM, @CantidadUMP, @precioUnitario,
						@PrecioUnitarioNeto,@precioNeto, @ObservacionesSTI
			END	
			close		remitoCur
			Deallocate	remitoCur -- por ahora...
			-- ahora el alta definitiva del comprobante
			-- antes que nada obtengo el total con IVA 13/06/2014

			    /*@TotalNeto          money,
			@TotalIVA           money,                       
			@TotalFactura       money,  
			@TotalNetoGravado        money = 0,          
			*/
			-- incorporo que contemple si hay items ajenos los cuales no sumar 14/04/2015
			Select	@sql = 'select @pPrecio = sum(tr.Precio * (([Tasa]/100)+1)), @pTotalIVA = sum(tr.Precio * (([Tasa]/100))), @pTotalNetoGravado = sum(tr.Precio) ' 
				+ N'FROM ##tempRemitosBIS TR inner join ' +
				@BasePhyDestino + N'.dbo.[FACProductos] P on p.idproducto = TR.CodigoPhysis collate  Modern_Spanish_CI_AS inner join ' +
				@BasePhyDestino + N'.dbo.[FACTipoTasas] TT on p.idtasaiva = TT.idtipotasa ' +
						'where tt.[fechaBaja] is null ' + 
				' and TR.usuarioSinMatricula = case when ' + cast(@ANTMCprocesaCCajena as varchar) + ' = 0 then 0 else TR.usuarioSinMatricula end ' +
				'		and TR.usuarioSinMatricula <> case when ' + cast(@ANTMCprocesaCCajena as varchar) + ' = 2 then 0 else 999999 end'


			Select @param = N'@pPrecio money output, @pTotalIVA money output, @pTotalNetoGravado money output'
			exec sp_executesql @sql, @param, @pPrecio = @TotalFactura output, @pTotalIVA = @TotalIVA output, @pTotalNetoGravado = @TotalNetoGravado output
			--select * from ##tempremitosbis
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Asi obtengo el total del comprobante con IVA, precio origen PHYSIS:' + isnull(@sql,'NULO')
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Total bruto: ' + cast(@TotalNetoGravado as varchar) + '; Total IVA: ' + cast(@TotalIVA as varchar) + '; Total con impuestos: ' + cast(@totalFactura as varchar)
			Select @TotalNeto = @totalNetoGravado
			-- Si el comprobante NO LLEVA IVA tengo que poner el IVA en cero e igualar el total al totalnetogravado
			If @BaseHija is not null
			begin
				Select @Sql = N'select top 1 @Presul=nocalculaautomatico from ' + @baseHija + '.dbo.empresa'
				if @modoDebug > 0 and @modoDebug < 3
					print 'Determino si plancho el IVA: ' + isnull(@sql,'NULO')
				exec sp_executesql @sql, N'@Presul int output', @Presul=@resul output
				if isnull(@resul,0)=1
				begin
					if @modoDebug > 0 and @modoDebug < 3
						Print 'iva planchado'						
					Select @TotalIVA = 0
					Select @TotalFactura = @TotalNetoGravado
				end
				else
					if @modoDebug > 0 and @modoDebug < 3
						Print 'iva sin tocar'						
			end
			else
				if @modoDebug > 0
					Print 'No hice nada con el IVA porque no hay base hija definida'

			/*
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Precio origen TWINS, asi que solo sumo el de los items'
			Select	@TotalFactura = (select sum(DPPrecioNeto)	FROM 	@DetallePhy)
			Select	@TotalNetoGravado = @TotalFactura
			*/
			--Select @ImporteTotal = isnull(@TotalFactura,0) *************************************************************************************
			Select @TotalNeto = @totalNetoGravado
			if @modoDebug > 0 and @modoDebug < 3
				print 'Total del comprobante (1): ' + cast(isnull(@totalfactura,'NULO') as varchar)
			-- cuando esto termina, me quedan igual un par de campos cargados
			-- @ObservacionesSIUP, @cuit
			
			select @ObservacionesSIUP = (
				SELECT top 1 'Codigo interno twins (NC_REM) nro ' + cast(CodigoRemitoTwins as varchar) + ' y codigo de carga ' + cast(rr.nro_carga as varchar)
				from @tempRemitos TR inner join dbo.[remitos resumen] RR on TR.CodigoremitoTwins = RR.nc_rem )
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Observaciones cargado con: ' + isnull(@ObservacionesSIUP, 'NULO, cuidado ahi')
			-- la consulta que trae los datos del cliente (uno x uno) usa el CUIT como clave
			-- y no toma en cuenta los que se hayan dado de baja (en physis, obviamente)
			-- ademas se queda con el nro de cuenta más grande (por si hay alguno de mas)
			select @IdCtaAuxi=  @codigoCliente
			-- la @IdCtaAuxi la veo mas arriba...
			-- Ahora traigo los datos del cliente, a lo dinamico
			Select @sql = N'Select top 1 @PIdTipoDocumento = t.IdTipoDocumento, @PNumeroDocumento = t.NumeroDocumento, ' +
				N'@PCategoriaIVA = t.CategoriaIVA From ' + @BasePhyDestino + N'.dbo.terceros t ' +
				N'where t.idctaAuxi = @PcodigoCliente and t.IdPpal = @PConstIdPpal and t.IdAuxi = @PConstIdAuxi '
			Select @param = N'@PIdTipoDocumento varchar(5) OUTPUT, @PNumeroDocumento varchar(12) OUTPUT, ' +
							N'@PCategoriaIVA varchar(2) OUTPUT, @PcodigoCliente varchar(12), ' +
							N'@PConstIdAuxi smallint, @PConstIdPpal smallint'
			exec sp_executesql @sql, @param, @PcodigoCliente = @codigoCliente, @PConstIdPpal = @ConstIdPpal, @PConstIdAuxi = @ConstIdAuxi, @PidTipoDocumento = @idTipoDocumento OUTPUT, @PNumeroDocumento = @NumeroDocumento OUTPUT,
				@PCategoriaIva = @CategoriaIVA OUTPUT
			if @modoDebug > 0 and @modoDebug < 3
			Begin
				Print 'Busqueda de datos del cliente, parte 1:'
				print @sql
			End
			-- Ahora el nombre del tercero:
			Select @sql = N'Select top 1 @PNombreTercero=c.Nombre From ' + @BasePhyDestino +
				N'.dbo.cuentasAuxi c where c.idctaAuxi = @PcodigoCliente and c.IdPpal = @PConstIdPpal and	c.IdAuxi = @PConstIdAuxi'
			Select @param = N'@PcodigoCliente varchar(12), @PConstIdAuxi smallint, @PConstIdPpal smallint, @PNombreTercero varchar(40) OUTPUT'
			exec sp_executesql @sql, @param, @PcodigoCliente =@CodigoCliente, @PConstIdAuxi =@ConstIdAuxi, @PConstIdPpal =@ConstIdPpal, @PNombreTercero = @NombreTercero OUTPUT
			if @modoDebug > 0 and @modoDebug < 3
			Begin
				Print 'Busqueda de datos del cliente, parte 2:'
				Print 'Estoy buscando el cliente con el codigo ' + CAST(isnull(@codigoCliente, 'NULLOOOO') as varchar)
				print @sql
			End

			-- Leo configuracion de condiciones de pago y vendedor: la reagrupacion y cond pago default y la reag de vendedor
			Select @sql = N'Select top 1 @PIdCondPago = IdCondPagoDefault, @PIdReagCPago = IdReagCPago, ' + 
				'@PIdReagVendedor = IdReagVendedor ' +
				'From ' + @BasePhyDestino + '.dbo.facParametros'
			Select @param = '@PidCondPago char(12) OUTPUT, @PIdReagCPago smallint OUTPUT, @PIdReagVendedor smallint OUTPUT'
			exec sp_executesql @sql, @param, @PidCondPago = @IdCondPago OUTPUT, @PIdReagCPago = @IdReagCondPago OUTPUT, @PIdReagVendedor = @IdReagVendedor OUTPUT
			if @modoDebug > 0 and @modoDebug < 3
			Begin
				Print 'Busqueda de condicion de pago default y reagrupacion de cond pago '
				Print @sql
				Print 'Condicion de pago obtenida: ' + cast(isnull(@idcondPago,'NULA') as varchar)
				Print 'Reagrupacion de condicion de pago obtenida: ' + cast(isnull(@IdReagCondPago,'NULA') as varchar)
				Print 'Reagrupacion de vendedor obtenida: ' + cast(isnull(@IdReagVendedor, 'NULA') as varchar)
			End
			-- Ahora trataremos de ver si tiene asignada una condicion de pago para este cliente
			-- uso un MIN() para quedarme con un solo valor. Por definicion siempre habra uno de todos modos.
			-- y si no tiene ninguna cargada, dejo la default 
			Select	@sql = N'Select @PIdCtaReagAuxi = isnull(min(RCA.IdCtaReagAuxi),''' + @idcondPago + ''') From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi  RCA Where RCA.IdPpal = @PIdPpal And ' +
						N'RCA.IdAuxi = @PIdAuxi And ' +
						N'RCA.IdReagAuxi = @PIdReagAuxi ' +
						N'and Rca.IdCtaAuxi= @PIdCtaAuxi'

/*			Select	@sql = N'Select @PIdCtaReagAuxi = isnull(min(CRA.IdCtaReagAuxi),''' + @idcondPago + ''') From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi RCA Inner Join ' + 
					@BasePhyDestino + '.dbo.CuentasReagrupacionAuxi CRA On ' +
						N'CRA.IdPpal = RCA.IdPpal And ' +
						N'CRA.IdAuxi = RCA.IdAuxi And ' +
						N'CRA.IdReagAuxi = RCA.IdReagAuxi ' +
						N'and CRA.IdCtaReagAuxi = RCA.IdCtaReagAuxi ' +
					N'Inner Join ' + @BasePhyDestino + '.dbo.CuentasAuxi CA On ' +
						N'RCA.IdAuxi = CA.IdAuxi And ' +
						N'RCA.IdCtaAuxi = CA.IdCtaAuxi ' +
						N'and RCA.IdPPal = CA.Idppal ' +
					N'Where RCA.IdPpal = @PIdPpal And ' +
						N'RCA.IdAuxi = @PIdAuxi And ' +
						N'RCA.IdReagAuxi = @PIdReagAuxi ' +
						N'and ca.IdCtaAuxi= @PIdCtaAuxi'
						*/


			Select	@param = N'@PIdPpal smallint, @PIdAuxi smallint, @PIdReagAuxi smallint, '
							+ N'@PIdCtaAuxi varchar(12), @PIdCtaReagAuxi varchar(12) output'
			exec sp_executesql @sql, @param, @ConstIdPpal,@ConstIdAuxi, @IdReagCondPago, @codigoCliente, @PIdCtaReagAuxi = @idcondPago
			if @modoDebug > 0 and @modoDebug < 3
			begin
				Print 'Busco cond de pago por reagrupacion: ' + isnull(@sql,'NULOOOO')
				Print 'Obtuve la cond de pago: ' + @IdCondPago
			end
			-- la frutilla del postre: el abastecedor! o como dice physis, vendedor

			Select	@sql = N'Set @PIdCtaReagAuxi = (select min(RCA.IdCtaReagAuxi) From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi  RCA Where RCA.IdPpal = ' + cast(@ConstIdPpal as varchar) + ' And ' +
						N'RCA.IdAuxi = ''' + cast(@ConstIdAuxi as varchar) + ''' And ' +
						N' RCA.IdReagAuxi = ''' + cast(@IdReagVendedor as varchar) +
						N''' and Rca.IdCtaAuxi= ''' + cast(@codigoCliente as varchar) + ''')'

/*
			Select	@sql = N'Select @PIdCtaReagAuxi = isnull(min(RCA.IdCtaReagAuxi),''NULL'') From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi  RCA Where RCA.IdPpal = @PIdPpal And ' +
						N'RCA.IdAuxi = @PIdAuxi And ' +
						N'RCA.IdReagAuxi = @PIdReagAuxi ' +
						N'and Rca.IdCtaAuxi= @PIdCtaAuxi'

					
			Select	@sql = N'Select @PIdCtaReagAuxi = isnull(CRA.IdCtaReagAuxi,''NULL'') From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi RCA Inner Join ' + 
					@BasePhyDestino + '.dbo.CuentasReagrupacionAuxi CRA On ' +
						N'CRA.IdPpal = RCA.IdPpal And ' +
						N'CRA.IdAuxi = RCA.IdAuxi And ' +
						N'CRA.IdReagAuxi = RCA.IdReagAuxi ' +
						N'and CRA.IdCtaReagAuxi = RCA.IdCtaReagAuxi ' +
					N'Inner Join ' + @BasePhyDestino + '.dbo.CuentasAuxi CA On ' +
						N'RCA.IdAuxi = CA.IdAuxi And ' +
						N'RCA.IdCtaAuxi = CA.IdCtaAuxi ' +
						N'and RCA.IdPPal = CA.Idppal ' +
					N'Where RCA.IdPpal = @PIdPpal And ' +
						N'RCA.IdAuxi = @PIdAuxi And ' +
						N'RCA.IdReagAuxi = @PIdReagAuxi ' +
						N'and ca.IdCtaAuxi= @PIdCtaAuxi'*/
			Select	@param = N'@PIdPpal smallint, @PIdAuxi smallint, @PIdReagAuxi smallint, '
							+ N'@PIdCtaAuxi varchar(12), @PIdCtaReagAuxi varchar(12) output'
			exec sp_executesql @sql, @param, @ConstIdPpal,@ConstIdAuxi, @IdReagVendedor, @codigoCliente, @PIdCtaReagAuxi = @idvendedor
			if @modoDebug > 0 and @modoDebug < 3
			begin
				Print 'Busco vendedor por reagrupacion: ' + isnull(@sql,'NULOV')
				Print 'Obtuve el vendedor (1): ' + isnull(@IdVendedor,'NULO')
				Print 'Use los parametros ' + cast(@ConstIdPpal as varchar) + ' - ' + cast(@ConstIdAuxi as varchar) + ' - ' + cast(@IdReagVendedor as varchar) + ' - ' + cast(@codigoCliente  as varchar) 
			end
			If @Idvendedor is null
				Select @idreagvendedor=NULL
			else
				Begin
					-- cuando el vendedor no existe en la DB de physis da un error feo...
					-- asi que haremos que de un error menos feo, pero error al fin - 17/06/2014
					Select @sql = N'Select @Presul=count(1) from ' + @BasePhyDestino + '.dbo.FacVendedores where IdCtaReagAuxi=@PIdCtaReagAuxi '
						+ ' and IdReagAuxi= @PIdReagVendedor and idPpal = @PidPpal and idauxi=@Pidauxi'
					Select	@param = N'@Presul int output, @PIdPpal smallint, @PIdAuxi smallint, @PIdReagVendedor smallint, @PIdCtaReagAuxi varchar(12)'
					exec sp_executesql @sql, @param, @Presul=@resul output,  @PIdPpal=@ConstIdPpal,@Pidauxi=@ConstIdAuxi, @PIdReagVendedor=@IdReagVendedor, @PIdCtaReagAuxi=@Idvendedor
					if isnull(@resul,0) = 0
					begin
						Select @ErrorMessage = 'El vendedor no existe en la DB Physis. Si aparece bien en pantalla se trata de un error en la DB; Nc_rem: ' + cast(@twncrem as varchar)
						raiserror (@ErrorMessage,16,1)
						if @@trancount > 0
							ROLLBACK tran 	
						return -1
					end
				End
			-- cuando se trata de un PRESUPUESTO tengo que diferenciarlo entre A y B
			-- como es el caso tambien con las facturas
			If (@ANTMCphyTipoCompro = 'P' or @ANTMCphyTipoCompro='F')	-- sobreescribo @ANDMCphyCompro segun corresponda A o B
				if (@CategoriaIva = '01' or @CategoriaIVA = '10' or @CategoriaIVA = '11')
					Select @ANTMCphyCompro = rtrim(ltrim(@ANTMCphyCompro)) + 'A'
				else 
					Select @ANTMCphyCompro = rtrim(ltrim(@ANTMCphyCompro)) + 'B'

			-- finalmente vamos con el alta del comprobante (que a su vez relacionara el detalle
			-- antes cargado) 		--	EL NUMERO lo cargo como parte del proceso
			If (@ANTMCphyTipoCompro = 'P') or (@ANTMCphyTipoCompro = 'F')-- solo ejecuto esto en caso de presupuestos
			Begin
				Select	@sql = 'exec ' + @BasePhyDestino + N'.dbo.spFillFACVencimiento_Manual_tmp ' +
					'''' + cast(@ConstIdPpal as varchar) + '''' + ', ' + 
					'''' + cast(@IdAuxi as varchar) + '''' + ', ' + 
					'''' + cast(@IdReagCondPago as varchar) + '''' + ', ' + 
					'''' + cast(@IdCondPago as varchar) + '''' + ', ' + 
					'''' + cast(@IdConexion as varchar)+ '''' 
				if @modoDebug > 0 and @modoDebug < 3
					Print 'Llamada a FillFACVencimiento_Manual_tmp ' + isnull(@sql,'NULA, changos')
				if @modoDebug > 0 
					Print @sql
				if @modoDebug < 2
				begin try
					exec sp_executesql @sql--, @param, @IdConexion
				end try
				begin catch
					print 'Error grave: '
					print ERROR_NUMBER() 
					print ERROR_SEVERITY() 
					print ERROR_STATE() 
					print ERROR_PROCEDURE() 
					print ERROR_LINE() 
					print ERROR_MESSAGE() 
					if @@trancount > 0
						ROLLBACK tran 	
					Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
					Raiserror (@ErrorMessage,16,1)					
					return -1
				end catch

				if @baseHija is not null
				begin
					Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
					if @modoDebug < 2
					begin try
						exec sp_executesql @sql--, @param, @IdConexion
					end try
					begin catch
						print 'Error grave: '
						print ERROR_NUMBER() 
						print ERROR_SEVERITY() 
						print ERROR_STATE() 
						print ERROR_PROCEDURE() 
						print ERROR_LINE() 
						print ERROR_MESSAGE() 
						if @@trancount > 0
							ROLLBACK tran 
						Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
						raiserror (@ErrorMessage,16,1)	
						return -1
					end catch

					if @modoDebug > 0
						print @sql
				end
			End
			--exec spFillFACVencimiento_Manual_tmp 1, @IdAuxi, @IdReagCPago, @IdCondPago, @IdConexion 
			Select @IdTipoComprobanteExt = @ANTMCphyCompro	-- comprobante externo, es el mismo
			Select @Numero = cast(@ANTMCSucursal as varchar) + CAST(@numero as varchar)
			--Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.SpFACStock_Insert_Update_Rem ' +

			if (@IdCtaAuxi is null) or (@IdTipoDocumento is null) or (@NumeroDocumento is null)
			BEGIN
				select @ErrorMessage = '1. Falta un dato (nulo): ' 
				if (@idctaauxi is null) select @ErrorMessage = @ErrorMessage + 'IdCtaAuxi'
				if (@IdTipoDocumento is null) select @ErrorMessage = @ErrorMessage + 'IdTipoDocumento de la cuenta ' + cast(@idctaauxi as varchar)
				if (@NumeroDocumento is null) select @ErrorMessage = @ErrorMessage + 'NumeroDocumento de la cuenta ' + cast(@idctaauxi as varchar)
				if @@trancount > 0
					rollback tran
				Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
				raiserror (@ErrorMessage,16,1)	
				return -1				
			end			
			Select @sql = 'exec ' + @BasePhyDestino + 
					case when @ANTMCphyTipoCompro='R' then N'.dbo.SpFACStock_Insert_Update_Rem '
						when @ANTMCphyTipoCompro='P' then N'.dbo.SpFACStock_Insert_Update_Fac '
						when @ANTMCphyTipoCompro='F' then N'.dbo.SpFACStock_Insert_Update_Fac '
						when @ANTMCphyTipoCompro='D' then N'.dbo.SpFACStock_Insert_Update_Ped '
					end +
				'''' + @ABMD + '''' + ', ' +
				'''' + cast(@IdCabecera as varchar) + '''' + ', ' +
				'''' + cast(isnull(@IdEjercicio,'NULO') as varchar) + '''' + ', ' +
				'''' + @ANTMCSucursal+ '''' + ', ' +
				'''' + CONVERT(nvarchar(30), @fecha, 126) + '''' + ', ' +
				'''' + ltrim(rtrim(@ANTMCphyCompro)) + '''' + ', '  +
				'''' + cast(@Numero as varchar) + '''' + ', ' + -- pto venta + nro
				'''' + cast(@IdAuxi as varchar) + '''' + ', ' +
				'''' + @IdCtaAuxi + '''' + ', ' +
				'''' + ltrim(rtrim(@IdTipoDocumento)) + '''' + ', ' +
				'''' + @NumeroDocumento + '''' + ', '  +
				case when @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P'
					then '''' + @TipoFactura + '''' + ', ' else '' end +
				'''' + @NombreTercero + '''' + ', ' +
				'''' + @CategoriaIVA + '''' + ', ' +
				'''' + @ObservacionesSIUP + '''' + ', ' +
				'''' + ltrim(rtrim(@ANTMCphyDeposito)) + '''' + ', ' +				
				case when @ANTMCphyTipoCompro='R' then 'NULL,'		-- Deposito "A" (o sea, remito para meter en otro deposito)
					else '' end +
				ISNULL('''' + cast(@IdAuxiListaPrecios as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdReagListaPrecios as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdListaPrecios as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdReagVendedor as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdVendedor as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdReagTransporte as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdTransporte as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdReagDescuento as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdDescuento1 as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@Descuento1 as varchar) + '''',' NULL ') + ', ' +				
				ISNULL('''' + cast(@IdDescuento2 as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@Descuento2 as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdReagObservaciones as varchar) + '''',' NULL ') + ', ' +								
				ISNULL('''' + cast(@IdCodObservaciones as varchar) + '''',' NULL ') + ', ' +
				'''' + ltrim(rtrim(@Referencia)) + '''' + ', ' + 
				ISNULL('''' + cast(@IdReagCondPago as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + ltrim(rtrim(cast(@IdCondPago as varchar))) + '''',' NULL ') + ', '  +		
				case when  @ANTMCphyTipoCompro='D' then ISNULL('''' + cast(@TotalNeto as varchar) + '''','0') 
					else  ISNULL('''' + cast(@FormaCosteo as varchar) + '''',' NULL ') 
				end + ', ' +		
				'''' + cast(@Alcance as varchar) + '''' + ', ' +
				'''' + cast(@ModoCarga as varchar) + '''' + ', ' +
				ISNULL('''' + cast(@IdMoneda as varchar) + '''',' NULL ') + ', ' +								
				ISNULL('''' + cast(@Serie as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@TasaCambio as varchar) + '''',' NULL ') + ', '
				If @ANTMCphyTipoCompro='R' 
					Select @sql = @sql + isnull('''' + cast(isnull(@TotalNetoGravado,0) as varchar) + '''',' NULL ') + ', '
				If @ANTMCphyTipoCompro='D' 
					Select @sql = @sql + ISNULL('''' + cast(@GrabarViaje as varchar) + '''','0') + ', '
				Select @sql = @sql +
					ISNULL('''' + cast(@IdUsuario as varchar) + '''',' NULL ') + ', ' +
					ISNULL('''' + cast(@IdConexion as varchar) + '''',' NULL ') + ', ' 
				-- Esto solo para FACturas y PResupuestos
				If @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P'
				begin
					--Select @sql = @sql + --'0,0,0,0,0,1,'
					--cast(@totalFactura as varchar)  + ', 0,0,0,' +
					--cast(@totalFactura as varchar)  + ', 1,'
					Select @sql = @sql +
						ISNULL('''' + cast(@TotalNeto as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@TotalIVA as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@TotalIVARNI as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@TotalPercepcionIVA as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@TotalFactura as varchar) + '''',' NULL ') + ',1, ' -- el ultimo es @Definitiva 
					--	@Definitiva         bit,      falta totalnetogravado y totalnetonogravado                  
					
				end 
				If @ANTMCphyTipoCompro='R'
				begin
					Select @sql = @sql + 
						ISNULL('''' + cast(@CodCampania as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@Planta as varchar) + '''',' NULL ') + ', ' 
				end
				If @ANTMCphyTipoCompro='D'
					Select @sql = @sql + 
							ISNULL('''' + cast(@forTranferWinsifac as varchar) + '''','0') + ', ' 
				else
					Select @sql = @sql + 
						ISNULL('''' + cast(@FechaExt as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@IdTipoComprobanteExt as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@NumeroExt as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@FechaVencimientoCAI as varchar) + '''',' NULL ') + ', ' +
						case when @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
							then '''' + '' + '''' + ', ' else '' end +		-- @NumeroCAI varchar(14)
						ISNULL('''' + cast(@IdPais as varchar) + '''',' NULL ') + ', ' +								
						ISNULL('''' + cast(@IdProvincia as varchar) + '''',' NULL ') + ', ' 
				if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
					Select @sql = @sql + '0,NULL,NULL,NULL,NULL,'''',0,'
				/*
					@GrabaNegativos	    bit,                   
					@CodCampania  	    smallint = Null ,         
					@IdMonedaPrint      Char(5) = Null,        
					@SeriePrint         TinyInt = Null,        
					@TasaPrint          float = Null,         
					@MensajeError       varchar(1000) output,      
					@EsMerma		bit = 0, 	            
				*/

				Select @sql = @sql + 'IDCABECERAREPL'
				If @ANTMCphyTipoCompro='D'
					Select @sql = @sql + ISNULL(''',' + cast(@CodCampania as varchar) + '''',', NULL ') + ', ' +
						ISNULL('''' + cast(@IdEstado as varchar) + '''',' NULL ') 

				--	ISNULL('''' + cast(@IdCabeceraRepl as varchar) + '''',' NULL ')		
				/* Si es factura o proforma todavia falta esto:
					@IdComprobanteSigesRepl 	int = 0,  
					@FElectronica 	  	bit = 0,      
					@FENroSolicitud 		int = 0,  
					@FEEsServicio 		smallint = 1,    
					@FEServicioFechaDesde 	DateTime = NULL,    
					@FEServicioFechaHasta 	DateTime = NULL,    
					@FERespuestaAFIP 		Varchar(500) = NULL, 
					@TotalNetoGravado		money = 0,           
					@TotalNetoNoGravado		money = 0, 
					@FechaIVA				datetime = Null, 
					@IdIdioma				Int = Null, 
					@MultiCuentaDeudor      bit = 0 
				*/
				if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
					Select @sql = @sql + ',IDCOMPROBANTESIGESREPL,0,0,1,NULL,NULL,NULL,' +
						ISNULL('''' + cast(@totalNetoGravado as varchar) + '''',' NULL ') + ', ' +
						ISNULL('''' + cast(@TotalNetoNoGravado as varchar) + '''',' NULL ') + ',NULL,NULL,0'
			if @modoDebug > 0 and @modoDebug < 3
				print 'Alta definitiva del comprobante:'

			-- en la ejecucion sobre la consolidada, ambos valores van en cero
			Select @sqlProvi =  replace(@sql,'IDCABECERAREPL','0')
			Select @sqlProvi =  replace(@sqlProvi,'IDCOMPROBANTESIGESREPL','0')
			if @modoDebug > 0
				print  isnull(@sqlProvi, 'Cadena de insercion de comprobante resulto nula (1)') 
			begin try
				delete from #CabecerasDevueltas
				if (@ANTMCphyTipoCompro='R' or @ANTMCphyTipoCompro='D') and exists (SELECT 1 FROM TempDB.INFORMATION_SCHEMA.COLUMNS
						where table_name like '#cabecerasdevueltas%' and column_name=N'idcomprobante')
					alter table #CabecerasDevueltas  
						drop column idcomprobante	--NumeroDefinitivo varchar(20) null, cabecera int null, idcomprobante varchar(20) null)
				if (@ANTMCphyTipoCompro='P' or @ANTMCphyTipoCompro='F') and 	not exists (SELECT 1 FROM TempDB.INFORMATION_SCHEMA.COLUMNS
						where table_name like '#cabecerasdevueltas%' and column_name=N'idcomprobante')
					alter table #CabecerasDevueltas  
						add idcomprobante varchar(20)
			end try
			begin catch
				Print 'Error grave antes de ejecutar el alta (cabecera - conso 2):'
				SELECT 
					@ErrorMessage = ERROR_MESSAGE(),
					@ErrorSeverity = ERROR_SEVERITY(),
					@ErrorState = ERROR_STATE();
				Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
						raiserror (@ErrorMessage,@ErrorSeverity,@ErrorState)	
				if @@trancount > 0 -- dentro de un catch no funciona
					ROLLBACK tran 	
				return -1
			end catch

			if @modoDebug < 2
			begin try
				delete from #CabecerasDevueltas
				insert #CabecerasDevueltas
					exec sp_executesql @sqlProvi
			end try
			begin catch
				if @@trancount > 0
					rollback tran
				Print 'Codigo de error:'
				Print @@Error
				Select @ErrorMessage = 'Error al dar de alta comprobante en Physis. Nc_rem: ' + cast(@twncrem as varchar)
				raiserror(@ErrorMessage,16,1)
				return -1
			end catch
			-- tomo la Idcabecera asignada en la consolidada, para usar en la hija
			select @IdCabeceraRepl = isnull((select top 1 cabecera from #CabecerasDevueltas),0)

			-- si el comprobante es un Remito y esta configurado para dar por cumplidos los propios
			-- en este punto proceso la base consolidada
			if @ANTMCphyTipoCompro = 'R' and @ANTMCcumplidoPropio = 1
			begin
				-- comparo CUIT de cliente y propio
				-- doy por cumplido si son iguales
				print 'Aqui daria por cumplido la cabecera ' + cast(@IdCabeceraRepl as varchar)
				print @NumeroDocumento

				Select	@Sql2	= N'SELECT top 1 @PMiCuit = NumeroDocumento from ' + @BasePhyDestino + N'.dbo.Empresa'
				Select	@param2	= N'@PMiCuit varchar(12) OUTPUT'
				Print 'B.' + @Sql2
				if @modoDebug > 0 and @modoDebug < 3
					Print @Sql2
				exec sp_executesql @sql2, @param2, @PMiCuit = @MiCuit Output
				Print @Micuit
				If @NumeroDocumento = @MiCuit
				begin
					Print 'Se da por cumplido la cabecera' + cast(@IdCabeceraRepl as varchar)
					Select @sql2 = N'Update ' + @BasePhyDestino + N'.dbo.FacStock Set estado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
					Set @sql2 = @sql2 + '; Update ' + @BasePhyDestino + N'.dbo.FacCabeceras Set Idestado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
					set @sql2 = @sql2 + '; INSERT INTO ' + @BasePhyDestino + N'.dbo.FACCabecerasEstados (IdCabecera, IdEstado, FechaHora, IdUsuario) '
						+N'VALUES (' + cast(@IdCabeceraRepl as varchar) +', 2, getdate(), ' + cast(@IdUsuario as varchar)  +') '
					if @modoDebug > 0 and @modoDebug < 3
						Print @Sql2
					print @sql2
					exec sp_executesql @sql2
				end
				else
					Print 'NO se da por cumplido la cabecera' + cast(@IdCabeceraRepl as varchar)
				-- para evitar tooooda esta saraza en la hija podria usar el valor de MiCUIT como flag para indicar 
				-- que hay que dar por cumplido

			end


			-- lo mismo con el IdComprobante de Siges
			if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
				Select @IdComprobanteSigesRepl = isnull((select top 1 idComprobante from #CabecerasDevueltas),0)
			if @modoDebug > 0 and @modoDebug < 3 
			begin
				print 'IdCabecera de replica: ' + cast (@IdCabeceraRepl as varchar)
				print 'IDComprobante de replica: ' + cast (@IdComprobanteSigesRepl as varchar)
			end
			Select @sql = replace(@sql,'IDCABECERAREPL',cast(@IdCabeceraRepl as varchar) )

			if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
				Select @sql = replace(@sql,'IDCOMPROBANTESIGESREPL',cast(@IdComprobanteSigesRepl as varchar) )
			-- si la cabecera es cero o menor, algo fallo al dar de alta en la consolidada
			-- si el comprobante es factura o proforma, ademas verifico si el comprobantesSiges es valido
			if @IdCabeceraRepl <= 0 or ((@ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P') and @IdComprobanteSigesRepl <=0)
			begin
				Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + cast(@twncrem as varchar)
				raiserror (@ErrorMessage,16,1)
				if @@trancount > 0
					ROLLBACK tran 	
				return -1
			end
			if @baseHija is not null
			begin
				Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
				if @modoDebug > 0 and @mododebug < 3
					print 'Alta en base hija:'
				if @modoDebug > 0
					print @sql
				if @modoDebug < 2
				begin try
					exec sp_executesql @sql--, @param, @IdConexion

					-- Repite lo del cumplimiento en la hija
					if (@ANTMCphyTipoCompro = 'R' and @ANTMCcumplidoPropio = 1) and (@NumeroDocumento = @MiCuit)
					begin
						Print 'Se da por cumplido la cabecera' + cast(@IdCabeceraRepl as varchar)
						Select @sql2 = N'Update ' + @BaseHija + N'.dbo.FacStock Set estado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
						Set @sql2 = @sql2 + '; Update ' + @BaseHija + N'.dbo.FacCabeceras Set Idestado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
						set @sql2 = @sql2 + '; INSERT INTO ' + @BaseHija + N'.dbo.FACCabecerasEstados (IdCabecera, IdEstado, FechaHora, IdUsuario) '
							+N'VALUES (' + cast(@IdCabeceraRepl as varchar) +', 2, getdate(), ' + cast(@IdUsuario as varchar)  +') '

						if @modoDebug > 0 and @modoDebug < 3
							Print @Sql2
						print 'Hija: ' + @sql2
						exec sp_executesql @sql2
					end
	

				end try
				begin catch
					print 'Error grave (cabecera - hija 1): '
					print ERROR_NUMBER() 
					print ERROR_SEVERITY() 
					print ERROR_STATE() 
					print ERROR_PROCEDURE() 
					print ERROR_LINE() 
					print ERROR_MESSAGE() 
					Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + ERROR_MESSAGE() + ' nc_rem: ' + cast(@twncrem as varchar)
					raiserror (@ErrorMessage,16,1)	
					if @@trancount > 0
						ROLLBACK tran 	
					return -1
				end catch
			end
			
			
			if @modoDebug > 0 and @modoDebug < 3
			Begin
				Print 'Condicion de pago 1: ' + cast(@IdReagCondPago as varchar)
				Print 'Condicion de pago 2: ' + cast(@IdCondPago as varchar)
				print isnull(@sql, 'Insercion de comprobante resulto nulo (1.5)')
			End
			Delete from @DetallePhy
			--Delete from ##tempRemitosBIS
			--Delete from @tempRemitos
		END
		-- Esto ya es el proceso de un nuevo comprobante, no confundir!

		-- 25/5 (fecha patria) encontre que una vez que manoseo la tabla tempRemitos en un proceso
		-- si en el siguiente proceso no la tenia que toquetear, ya la dejé alterada
		-- por ende estoy agregando lineas para que al iniciar un proceso la tabla tenga todo fresquito
		Delete from @tempRemitos
		insert @tempRemitos
			exec dbo.syncDespachoDetalle @TWNcRem

		-- Obtengo la base consolidada de destino y la base relacionada
		-- ============================================================
		Select	@BasePhyDestino = (select top 1 
			case isnull(servidor,'') when '' then '' else QUOTENAME(servidor) + '.' end + quotename(base)
			from dbo.syncServidoresPhysis where id=@MCphyBase)
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Base destino Phy: ' + @BasePhyDestino
		-- ahora busco la base asociada a este comprobante
		--Conversado con EOD: solo uso UNA hija. Si no se definio ninguna, naranja, no aplica a las demas.
		If (@MCphyTipoCompro = 'P' or @MCphyTipoCompro = 'F')
			Select @sql =	N'Select @PBaseHija = replace(baseRelacionada,''Siges'',''Sifac'')  From ' + @BasePhyDestino +
				 N'.dbo.TiposComprobante Where IdTipoComprobante like rtrim(ltrim(@PComp)) + ''%'' '
		else
			Select @sql =	N'Select @PBaseHija = replace(baseRelacionada,''Siges'',''Sifac'')  From ' + @BasePhyDestino +
				 N'.dbo.TiposComprobante Where IdTipoComprobante= @PComp'
		Select @param = N'@PComp varchar(5), @PBaseHija varchar(100) output'
		exec sp_executesql @sql, @param, @MCphyCompro, @PBaseHija = @BaseHija output
		Select @BaseHija = left(@BasePhyDestino,patindex('%.%',@BasePhyDestino)) + quotename(@BaseHija)
		if isnull(@BaseHija,'[]')='[]' collate Modern_Spanish_CI_AS
			select @baseHija=NULL		
		if @modoDebug > 0 and @modoDebug < 3
		Begin
			Print @sql
			Print 'Determinacion de bases hijas de ' + @BasePhyDestino
			Print 'En la busqueda de una unica base relacionada se leyó el valor ' + isnull(@BaseHija,'NULO')
		End
		-- Leo configuracion de condiciones de pago y vendedor: la reagrupacion y cond pago default y la reag de vendedor
		Select @sql = N'Select top 1 @PIdCondPago = IdCondPagoDefault, @PIdReagCPago = IdReagCPago, ' + 
			'@PIdReagVendedor = IdReagVendedor ' +
			'From ' + @BasePhyDestino + '.dbo.facParametros'
		Select @param = '@PidCondPago char(12) OUTPUT, @PIdReagCPago smallint OUTPUT, @PIdReagVendedor smallint OUTPUT'
		exec sp_executesql @sql, @param, @PidCondPago = @IdCondPago OUTPUT, @PIdReagCPago = @IdReagCondPago OUTPUT, @PIdReagVendedor = @IdReagVendedor OUTPUT
		if @modoDebug > 0 and @modoDebug < 3
		Begin
			Print 'Busqueda de condicion de pago default y reagrupacion de cond pago '
			Print @sql
			Print 'Condicion de pago obtenida: ' + cast(isnull(@idcondPago,'NULA') as varchar)
			Print 'Reagrupacion de condicion de pago obtenida: ' + cast(isnull(@IdReagCondPago,'NULA') as varchar)
			Print 'Reagrupacion de vendedor obtenida: ' + cast(isnull(@IdReagVendedor, 'NULA') as varchar)
		End
		-- Ahora trataremos de ver si tiene asignada una condicion de pago para este cliente
		-- uso un MIN() para quedarme con un solo valor. Por definicion siempre habra uno de todos modos.
		-- y si no tiene ninguna cargada, dejo la default 

			Select	@sql = N'Select @PIdCtaReagAuxi = isnull(min(RCA.IdCtaReagAuxi),''' + @idcondPago + ''') From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi  RCA Where RCA.IdPpal = @PIdPpal And ' +
						N'RCA.IdAuxi = @PIdAuxi And ' +
						N'RCA.IdReagAuxi = @PIdReagAuxi ' +
						N'and Rca.IdCtaAuxi= @PIdCtaAuxi'
/*
		Select	@sql = N'Select @PIdCtaReagAuxi = isnull(min(CRA.IdCtaReagAuxi),''' + ltrim(rtrim(@idcondPago)) + ''') From ' + @BasePhyDestino + 
				N'.dbo.ReagrupacionCuentasAuxi RCA Inner Join ' + 
				@BasePhyDestino + '.dbo.CuentasReagrupacionAuxi CRA On ' +
					N'CRA.IdPpal = RCA.IdPpal And ' +
					N'CRA.IdAuxi = RCA.IdAuxi And ' +
					N'CRA.IdReagAuxi = RCA.IdReagAuxi ' +
					N'and CRA.IdCtaReagAuxi = RCA.IdCtaReagAuxi ' +
				N'Inner Join ' + @BasePhyDestino + '.dbo.CuentasAuxi CA On ' +
					N'RCA.IdAuxi = CA.IdAuxi And ' +
					N'RCA.IdCtaAuxi = CA.IdCtaAuxi ' +
					N'and RCA.IdPPal = CA.Idppal ' +
				N'Where RCA.IdPpal = @PIdPpal And ' +
					N'RCA.IdAuxi = @PIdAuxi And ' +
					N'RCA.IdReagAuxi = @PIdReagAuxi ' +
					N'and ca.IdCtaAuxi= @PIdCtaAuxi'
					*/
		Select	@param = N'@PIdPpal smallint, @PIdAuxi smallint, @PIdReagAuxi smallint, '
						+ N'@PIdCtaAuxi varchar(12), @PIdCtaReagAuxi varchar(12) output'
		exec sp_executesql @sql, @param, @ConstIdPpal,@ConstIdAuxi, @IdReagCondPago, @codigoCliente, @PIdCtaReagAuxi = @idcondPago
		if @modoDebug > 0 and @modoDebug < 3
		begin
			Print 'Busco cond de pago por reagrupacion: ' + isnull(@sql,'NULOOOO')
			Print 'Obtuve la cond de pago: ' + @IdCondPago
		end
		-- la frutilla del postre: el abastecedor! o como dice physis, vendedor

					Select	@sql = N'Set @PIdCtaReagAuxi = (select min(RCA.IdCtaReagAuxi) From ' + @BasePhyDestino + 
					N'.dbo.ReagrupacionCuentasAuxi  RCA Where RCA.IdPpal = ' + cast(@ConstIdPpal as varchar) + ' And ' +
						N'RCA.IdAuxi = ''' + cast(@ConstIdAuxi as varchar) + ''' And ' +
						N' RCA.IdReagAuxi = ''' + cast(@IdReagVendedor as varchar) +
						N''' and Rca.IdCtaAuxi= ''' + cast(@codigoCliente as varchar) + ''')'
/*

		Select	@sql = N'Select @PIdCtaReagAuxi = isnull(CRA.IdCtaReagAuxi,''NULL'') From ' + @BasePhyDestino + 
				N'.dbo.ReagrupacionCuentasAuxi RCA Inner Join ' + 
				@BasePhyDestino + '.dbo.CuentasReagrupacionAuxi CRA On ' +
					N'CRA.IdPpal = RCA.IdPpal And ' +
					N'CRA.IdAuxi = RCA.IdAuxi And ' +
					N'CRA.IdReagAuxi = RCA.IdReagAuxi ' +
					N'and CRA.IdCtaReagAuxi = RCA.IdCtaReagAuxi ' +
				N'Inner Join ' + @BasePhyDestino + '.dbo.CuentasAuxi CA On ' +
					N'RCA.IdAuxi = CA.IdAuxi And ' +
					N'RCA.IdCtaAuxi = CA.IdCtaAuxi ' +
					N'and RCA.IdPPal = CA.Idppal ' +
				N'Where RCA.IdPpal = ' + cast(@ConstIdPpal as varchar) + ' And ' +
					N'RCA.IdAuxi = ' + cast(@ConstIdAuxi as varchar) + ' And ' +
					N'RCA.IdReagAuxi = ' + cast(@IdReagVendedor as varchar) +
					N' and ca.IdCtaAuxi= ' + cast(@codigoCliente as varchar)
					*/
		Select	@param = N'@PIdCtaReagAuxi varchar(12) output'
		exec sp_executesql @sql, @param, @PIdCtaReagAuxi = @idvendedor output
		if @modoDebug > 0 and @modoDebug < 3
		begin
			Print 'Busco vendedor por reagrupacion: ' + isnull(@sql,'NULOV')
			Print 'Obtuve el vendedor (2): ' + isnull(@IdVendedor, 'NULO')
			--Print 'Use los parametros ' + cast(@ConstIdPpal as varchar) + ' - ' + cast(@ConstIdAuxi as varchar) + ' - ' + cast(@IdReagVendedor as varchar) + ' - ' + cast(@codigoCliente  as varchar) 
		end
		If @Idvendedor is null
				Select @idreagvendedor=NULL
		else
			Begin
				-- cuando el vendedor no existe en la DB de physis da un error feo...
				-- asi que haremos que de un error menos feo, pero error al fin - 17/06/2014
				Select @sql = N'Select @Presul=count(1) from ' + @BasePhyDestino + '.dbo.FacVendedores ' + 
				' where IdCtaReagAuxi=''' + ltrim(rtrim(cast(@idvendedor as varchar))) + '''' +
					 ' and IdReagAuxi='''+ ltrim(rtrim(cast(@IdReagVendedor as varchar)))  +
					 ''' and idPpal = ''' + ltrim(rtrim(cast(@ConstidPpal as varchar))) + 
						 ''' and idauxi=''' + ltrim(rtrim(cast(@Constidauxi as varchar))) + ''''
				Select	@param = N'@Presul int output'
				exec sp_executesql @sql, @param, @Presul=@resul output
				if isnull(@resul,0)= 0
				begin
					print @sql
					Select @ErrorMessage = 'El vendedor ' + ltrim(rtrim(cast(@idvendedor as varchar))) + ' no existe en la DB Physis ' + @basePhydestino + '. Nc_rem: ' + cast(@twncrem as varchar)
					raiserror (@ErrorMessage ,16,1)
					if @@trancount > 0
						ROLLBACK tran 	
					return -1
				end
			End
		-- ============================================================
		/*
		Para cada comprobante tengo aqui la parametrizacion. 
		
		Para cada uno... leo la config y doy de alta el detalle.
		Al comienzo del bucle (y justo despues de que termina) tendria que confirmar el alta del comprobante
		
		*/
		
		-- Empiezo a retocar la data segun el dibujo necesario.
		-- Quien soy? lo tengo en @DBActual
		-- Ahora me fijo si tengo parametrizado usar alguna otra DB para producto, peso o precio:

		Select @varBase = Numero from @ServerTwins where nombreBase = db_name() collate latin1_general_cs_as 
		If cast(@MCtwBaseCodPro as int) <> @varBase
		Begin
			Select	@sql = ' exec ' + (Select max(nombreBase) from @serverTwins where numero=@MCtwBaseCodPro) + '.dbo.syncDespachoDetalle ''' + cast(@TWNcRem as varchar) + ''''
			if @modoDebug > 0 and @modoDebug < 3
			begin
				Print 'La base actual es ' + DB_NAME() + ' pero la base de producto es COD:' + cast(@MCtwBaseCodPro as varchar)
				Print 'Sentencia SQL de ejecucion de llenado de temporal BIS: '
				Print @sql
			end
			Insert ##tempRemitosBIS 
				exec (@sql)
			if @modoDebug > 0 and @modoDebug < 3
			begin
				Select @AuxCuentaFilas = isnull((Select count(1) from ##tempRemitosBIS),0)
				Print '0. Inserte ' + CAST(@AuxCuentaFilas as varchar) + ' filas en remitosBIS'
			end
			-- ya tengo la data de la carga detallada en la tabla @tempRemitosBIS
			-- llego el momento de manosear la principal con los datos de la BIS

			--With TRM as (select * from @tempRemitos)
			--UPDATE	x
			--Set		codigo = TRB.codigo
			--From	TRM AS x
			--Inner join ##tempRemitosBIS AS TRB on 
			--	(TRM.codbar = TRB.codbar)
			Update	TR
			Set		TR.codigo = TB.codigo
			From	@tempRemitos TR inner join ##tempRemitosBIS TB on
				(TR.codbar = TB.codbar collate Modern_Spanish_CI_AS)
		End
		Select @varBase = Numero from @ServerTwins where nombreBase = DB_Name() 
		If cast(@MCtwBasePeso as int) <> @varBase
		Begin
			Select	@sql = ' exec ' + (Select nombreBase from @serverTwins where numero=@MCtwBasePeso) + '.dbo.syncDespachoDetalle ''' + cast(@TWNcRem as varchar) + ''''
			if @modoDebug > 0 and @modoDebug < 3
			begin
				Print 'La base actual es ' + DB_NAME() + ' pero la base de peso es COD: ' + cast(@MCtwBaseCodPro as varchar)
				Print 'Sentencia SQL de ejecucion de llenado de temporal BIS: '
				Print @sql
			end
			--print 'estoy por cargar los datos de los pesos'
			-- si son el mismo origen, entonces no necesito volver a cargarlo
			--If @MCtwBasePeso <> @MCtwBaseCodPro
			--Begin
			--	print 'me copio los datos:'
				Delete from ##tempRemitosBIS
				Insert into ##tempRemitosBIS
					exec (@sql)
			--	Select @AuxCuentaFilas = isnull((Select count(1) from ##tempRemitosBIS),0)
			--	if @modoDebug > 0
			--		Print '2. Inserte ' + CAST(isnull(@AuxCuentaFilas,0) as varchar) + ' filas en remitosBIS'
			--End
			-- ya tengo la data de la carga detallada en la tabla @tempRemitosBIS
			-- llego el momento de manosear la principal con los datos de la BIS
			Update	TR
			Set		TR.peso = TB.peso
			From	@tempRemitos TR inner join ##tempRemitosBIS TB on
				(TR.codbar = TB.codbar collate Modern_Spanish_CI_AS)
			--Update	@tempRemitos
			--Set		@tempRemitos.peso = ##tempRemitosBIS.peso
			--From	@tempRemitos inner join ##tempRemitosBIS
			--	on (@tempRemitos.codbar = ##tempRemitosBIS.codbar)
		End
		-- Este bloque sirve para que tome los precios de una base distinta a la del despacho
		select @varBase = Numero from @ServerTwins where nombreBase = DB_Name() collate latin1_general_cs_as 
		If cast(@MCtwBasePrecio as int) <> @varBase 
			and @MCphyPrecio = 0 and @MCcriterioPrecio > 0 -- Si el precio es de twins y esta habilitado su manejo
		Begin
			Select	@sql = ' exec ' + (Select nombreBase from @serverTwins where numero=@MCtwBasePrecio) + '.dbo.syncDespachoDetalle ''' + cast(@TWNcRem as varchar) + ''''
			If @modoDebug > 0 and @modoDebug < 3
			begin
				Print 'La base actual es ' + DB_NAME() + ' pero la base de precios es COD:' + cast(@MCtwBaseCodPro as varchar)
				Print 'Sentencia SQL de ejecucion de llenado de temporal BIS: '
				Print @sql
			end
			-- si son el mismo origen, entonces no necesito volver a cargarlo
			If @MCtwBasePeso <> @MCtwBasePrecio
			Begin
				Delete from ##tempRemitosBIS
				Insert into ##tempRemitosBIS
					exec (@sql)		
				if @modoDebug > 0 and @modoDebug < 3
				begin
					Select @AuxCuentaFilas = isnull((Select count(1) from ##tempRemitosBIS),0)
					Print '1. Inserte ' + CAST(@AuxCuentaFilas as varchar) + ' filas en remitosBIS, actualizando PRECIOS'
				end
			End 
			-- ya tengo la data de la carga detallada en la tabla @tempRemitosBIS
			-- llego el momento de manosear la principal con los datos de la BIS
			Update	TR
			Set		TR.precio = TB.precio
			From	@tempRemitos TR inner join ##tempRemitosBIS TB on
				(TR.codbar = TB.codbar collate Modern_Spanish_CI_AS)
		End
		-- Ya logré retocar todo lo necesario en el origen de datos, el detalle del comprobante

		Delete from ##tempRemitosBIS 
		-- No le cargo el precio unitario, sino solo el calculado
		Insert ##tempRemitosBIS (remitoTwins,nroCarga,Codigo,Descripcion,Unidades,Peso,Usuario,Tropa,CodigoRemitoTwins,
								CodigoPhysis,CodigoInterno,cuit,codigoCliente,usuarioSinMatricula,precio)
			Select [remitoTwins], [nroCarga], [Codigo], 
				[Descripcion], sum([Unidades]), sum([Peso]), Usuario, [Tropa],
				[CodigoRemitoTwins], [CodigoPhysis], [CodigoInterno], 
				[cuit],	[codigoCliente], [usuarioSinMatricula],
				sum(precio * peso)	
			From @tempRemitos
			Group by [remitoTwins], [nroCarga], [Codigo], 
				[Descripcion], Usuario, [Tropa], [CodigoRemitoTwins], [CodigoPhysis], [CodigoInterno], 
				[cuit],	[codigoCliente], [usuarioSinMatricula]
				,precio -- 24-10-2014
		if @modoDebug > 0 and @modoDebug < 3
		begin
			Select @AuxCuentaFilas = (Select count(1) from ##tempRemitosBIS)
			Print '-1.Inserte ' + CAST(@AuxCuentaFilas as varchar) + ' filas en remitosBIS, al terminar los retoques de mezcla de bases'
		end
		-- Si corresponde, tengo que manosear el IVA del precio
		-- 0: Precio no se usa
		-- 1: precio no se toca
		-- 2: Precio hay que agregar IVA
		-- 3: Precio hay que sacarle IVA
		-- Asi que solamente en los casos 2 y 3 hago algo
		If @MCcriterioPrecio in ('2','3')
		begin
			Select	@sql = N'Update	##tempRemitosBIS Set precio = cast(precio '
			Select	@sql = @sql + case when @MCcriterioPrecio = '2' then '*' else '/' end
			Select	@sql = @sql + N' (([Tasa]/100)+1) as decimal(10,5)) FROM ' +
				@BasePhyDestino + N'.[dbo].[FACTipoTasas] TT inner join ' +
				@BasePhyDestino + N'.[dbo].[FACProductos] P on p.idtasaiva = TT.idtipotasa where tt.[fechaBaja] is null ' +
				N' and p.idproducto = ##tempRemitosBIS.CodigoPhysis collate  Modern_Spanish_CI_AS'
			exec sp_executesql @sql
			if @modoDebug > 0 and @modoDebug < 3
			begin
				Print 'Ajuste de precios tocando IVA:'
				Print @sql
			end
		end
		-- Como de twins traigo el precio final sumado, con esto hago el calculo inverso del precio unitario
		Update  ##tempRemitosBis
		Set		precioUnit = Precio / Peso
		Where	peso <> 0 and precio <> 0
		-- y como no quiero nulos, si falto algun precio, lo dejo en cero
		Update	##tempRemitosBis
		Set		precioUnit = 0
		where	precioUnit is null
		-- Ahora en @tempRemitosBIS tengo el resumen, lo que se carga en Physis
		-- Determinacion del nro de comprobante
		--select * from ##tempRemitosBIS
		Select	@numero = (select top 1 rtrim(ltrim(cast(RemitoTwins as varchar))) from ##tempRemitosBIS where RemitoTwins is not null)
		Select	@numero = replicate('0', (8 - len(@numero))) + cast(@numero as varchar)
		if @modoDebug > 0 and @modoDebug < 3
		begin
			--print @sql
			Print 'Numero asignado:: ' + cast(isnull(@Numero, 'NULO') as varchar)
		end
		-- Sigue: reemplazar los nros de tropa (Twins) por los nros de partida (Physis)

		if object_id('tempdb..##TropasBuscadas') is not null
			drop table ##TropasBuscadas
		Create table ##TropasBuscadas (
				NumTropa	int primary key,
				IdPartida	char(12),
				Usuario		int,
				anio		char(2)
		)
		-- Para distinguir entre tropas propias y tropas provenientes de otros frigos, voy a usar el USUARIO
		-- cuando las tropas llegaron de otro frigo, el prefijo de la tropa es distinto
		-- entonces para hacer coincidir la tropa, con encontrar el prefijo correcto seria suficiente
		-- 10/7/15 quito del where tropa<>'0' esta condicion AND: tropa in (select nro_tropa from llegada_tropa_resumen) and
		-- porque esta dejando fuera las tropas de otros frigos
		-- Agrego una mencion al CriterioSufijoTropa. Si esta definido que se usa, entonces agrega un cero al final de la tropa
		-- porque para hacer el match con la partida en physis que sea de la primer subtropa.
		-- Todas las salidas coincidiran con la primer subtropa (la cero). 07/08/15
		
		-- 30/10/2015 puede que lleguen tropas con subtropa a este punto
		-- en ese caso tengo que quitarle el punto, pero no agregarle el cero
		-- busco el punto 
		Insert ##TropasBuscadas (NumTropa, usuario)
			Select distinct case when @CONSTCriterioSufijoTropa=0 then cast(tr.tropa as varchar) 
					else 
						case when charindex('.',cast(tr.tropa as varchar))=0
							then cast(tr.tropa as varchar) + '0'
							else replace(cast(tr.tropa as varchar),'.','')  end
							end, isnull(SSP.PTT,TR.usuario) 
				from (select tropa, usuario from ##tempRemitosBIS where  tropa<>'0') TR 
				left join
				(select distinct prefijoTropasTwins PTT, idUsuarioFaena IDU from syncServidoresPhysis) SSP on TR.usuario=SSP.IDU

		-- tengo que ser consecuente con esto, asi que a ##tempremitosBIS tambien le acomodo la tropa con el sufijo
		-- 30/10/15 Si la tropa viene de esta forma "1222.0" entonces no tengo que agregarle el digito final
		-- y ademas tengo que sacarle el punto

		if @CONSTCriterioSufijoTropa=1
			update ##tempRemitosBIS 
			set tropa=cast(tropa as varchar) + '0'
			where patindex('%.%',cast(tropa as varchar))=0
		update ##tempRemitosBIS 
		set tropa=replace(cast(tropa as varchar),'.','')

		update ##tempRemitosBIS set tropa='0' where tropa='00'
		update ##TropasBuscadas set NumTropa='0' where NumTropa='00'
		-- Antes directamente buscaba el usuario tal como esta en Twins
		-- Luego (13/06/2014) lo cambié para que busque el PrefijoTropasTwins que le estoy asignando en Physis
		-- Lease asi: busque la tropa tal como esta en Twins con el prefijo de usuario que uso en Physis para ese usuario en Twins
		-- Esto deberia lidiar con el caso en que el prefijo de usuario que uso para las partidas en Physis (al que llamo prefijoTropasTwins)
		-- sea distinto del campo "usuario" tal como lo trae syncDespachoDetalle
		-- Antes hacia esto Select distinct tropa, usuario from ##tempRemitosBIS
		-- completo con las partidas que ya pude determinar 
		-- NO HAGO UNA BUSQUEDA EXACTA porque a menos que implementan trazabilidad por media res
		-- Twins no puede determinar a que media corresponde cada cuarto. 
		-- si me quisiera complicar la vida podria distinguir las medias de los cuartos y buscar por media las tropas de ellas.
		Select @sql = N'update ##TropasBuscadas ' +
					N'Set idPartida = (select max(idpartida) ' +
					N'from ' + @BasePhyDestino + N'.dbo.facPartidas ' +
					N'where fechaBaja is null ' +
					N' and idpartida like  ''%''' + 
					N'		+ ltrim(rtrim(cast(Usuario as varchar))) ' +
					N'			+ ltrim(rtrim(replicate(''0'',' + cast(@AnchoTropa as varchar) + N'-len(NumTropa)))) ' +
					N'			+ cast(NumTropa as varchar) collate SQL_Latin1_General_CP1_CI_AS) ' +
					N'where Numtropa <> 0 '
		if @modoDebug > 0 and @modoDebug < 3
		begin
			Print 'Buscamos los nros de las de tropas que faltan en la base: ' + @BasePhyDestino
			Print @sql
		end
		exec sp_executesql @sql
		-- y ahora a las que no haya encontrado las busco SIN el prefijo, por ser tal vez tropas importadas de otro frigo

		Select @sql = N'update ##TropasBuscadas ' +
					N'Set idPartida = (select max(idpartida) ' +
					N'from ' + @BasePhyDestino + N'.dbo.facPartidas ' +
					N'where fechaBaja is null ' +
					N' and idpartida like  ''%''' + 
					N'			+ ltrim(rtrim(replicate(''0'',' + cast(@AnchoTropa as varchar) + N'-len(NumTropa)))) ' +
					N'			+ cast(NumTropa as varchar) collate SQL_Latin1_General_CP1_CI_AS) ' +
					N'where Numtropa <> 0 and idpartida is null'
		if @modoDebug > 0 and @modoDebug < 3
		begin
			Print 'Buscamos los nros de las de tropas que faltan en la base: ' + @BasePhyDestino
			Print @sql
		end
		exec sp_executesql @sql
		-- y ahora las pegamos en la @tempRemitosBIS
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Completamos las tropas con las encontradas'
		Update	##tempRemitosBis
		Set		tropa = ##TropasBuscadas.idPartida
		From	##TropasBuscadas
		Where	tropa = ##TropasBuscadas.NumTropa 
			and tropa <> 0 
			and Tropa IS Not null
		-- Las que dejo como nulas son las que no toco el update dinamico
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Y las que no encontro las marcamos'
		Update	##tempRemitosBis
		Set		tropa = @CONSTPartidaNoEncontrada
		where	tropa is null
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Completamos los codigos de producto'
		-- Sigue el tratamiento de los codigos de producto
		-- Reemplazo los codigos de producto de twins con el de physis en codAdm
		update	##tempRemitosBIS 
		set		codigoPhysis	= @ConstProductoNoEncontrado
		where	codigoPhysis='' 
		update	##tempRemitosBIS 
		set		codigoPhysis	= @ConstProductoNoEncontrado
		where	isnull(codigoPhysis,'0')='0' 
		if @modoDebug > 0 and @modoDebug < 3
			Print 'SD. Termine de completar los codigos de producto'
		-- Luego la relacion tropas-producto en Phy
		-- Las tropas ya las verifiqué antes
		-- Ahora tambien me fijo si los productos estan relacionados con la tropa.
		Select @sql=N'Select @PresulVC = coalesce(@PresulVC + '','' + cast(codigoPhysis as varchar),cast(codigoPhysis as varchar)) from ##tempRemitosBIS where codigoPhysis collate Modern_Spanish_CI_AS not in ' +
			'(select IdProducto from ' + @BasePhyDestino + ' .dbo.FacProductos)'
		Select @param=N'@PresulVC varchar(100) output'
		if @modoDebug > 0 and @modoDebug < 3
		begin
			Print 'SD. Reviso si hay productos que no estan en Physis'
			Print @sql
		end
		begin try
			exec sp_executesql @sql,@param,@PresulVC=@resulVC output
		end try
		begin catch
			print 'Error grave (revisando productos): '
			print ERROR_NUMBER() 
			print ERROR_SEVERITY() 
			print ERROR_STATE() 
			print ERROR_PROCEDURE() 
			print ERROR_LINE() 
			print ERROR_MESSAGE() 
			if @@trancount > 0
				ROLLBACK tran 
			Select @ErrorMessage='Error al tratar de procesar por interfaz. '+ ERROR_MESSAGE() 
			raiserror (@ErrorMessage,16,1)
			return -1
		end catch
		if isnull(@resulvc,'0')<>'0'
		begin
			if @@trancount>0
				rollback tran
			Select @ErrorMessage = 'Hay codigos de producto (codadm) que no aparecen en Physis: '+ ltrim(rtrim(cast(@resulVC as varchar(100))))
			raiserror (@ErrorMessage,16,1)
			return -1
		end
		SELECT 	@sql = 'Insert BASEPRINCIPAL.dbo.FACPartidasPorProducto (IdPartida, IdPlanProducto, IdProducto) ' +
				'SELECT distinct Tropa,''' + cast(@IdPlanProducto as varchar)
				+ ''' , CodigoPhysis from ##tempRemitosBIS where tropa<> ''0'' and not exists ' +
				N'(select 1 from BASEPRINCIPAL.dbo.facPartidasPorProducto fp where ' +
				N' fp.IdPlanProducto =' + cast(@IdPlanProducto as varchar) + ' and fp.idpartida=Tropa ' +
				 ' collate SQL_Latin1_General_CP1_CI_AS and fp.Idproducto=CodigoPhysis collate SQL_Latin1_General_CP1_CI_AS)'
		-- el where TROPA <> 0 es porque me interesan solo las tropas de colgado
		select @sqlProvi = replace(@sql, 'BASEPRINCIPAL', @BasePhyDestino)
		if @modoDebug > 0  and @modoDebug < 3
			Print 'SD. Asi cargo las relaciones que faltan:' 		
		if @modoDebug > 0
			print @sqlProvi
		if @modoDebug < 2
			exec sp_executesql @sqlProvi
		if @baseHija is not null
		begin
			Select @sqlProvi = replace(@sql, 'BASEPRINCIPAL', @BaseHija)
			if @modoDebug > 0 
				print @sqlProvi 
			if @modoDebug < 2
				exec sp_executesql @sqlProvi 
		end

		If @ModoDebug > 0 and @modoDebug < 3
			Print 'Termine con las partidas'
		
		-- Finalmente los precios, que se deben aplicar si no hay precio definido previamente
		/*
		Lo que sigue es para "traer" la lista de precios a afectar al pedido
		Son tres las variables a cargar:
		@IdAuxiListaPrecios
		@IdReagListaPrecios
		@IdListaPrecios
		*/
		-- tengo que identificar el nro de cuenta del cliente para buscar la lista de precios...
		-- por eso veo esto ahora, aunque luego el mismo campo lo vuelvo a usar para
		-- dar de alta el comprobante
		Select @IdCtaAuxi = (select top 1 codigoCliente from ##tempRemitosBIS)
		-- Esta estructura me sirve para dos cosas. Necesito que tenga los codigos de producto, pa'empezar:
		Delete from ##ProductosBuscados
		Insert ##ProductosBuscados (PBIdProducto)
			Select	Distinct [CodigoPhysis]
			From	##tempRemitosBIS
		If @MCphyPrecio = '1' and @MCcriterioPrecio <> '0' -- significa 1: Precio Physis 0: no toco precios
		Begin
			-- Primero voy a poner los precios en cero, asi si falta alguno, 
			-- o si directamente no tiene lista de precios asignada ni por default, 
			-- los precios no quedan en los valores determinados por twins (JH, 27-05-2014)
			Update ##tempRemitosBIS
			Set precio = 0, precioUnit = 0
			-- Me fijo si tiene lista de precios cargada
			-- Esto va a devolver los datos de una lista de precios
			-- a la que el cliente este relacionado
			-- siempre que este vigente la lista
			Select	@sql = N'select	@PIdAuxiListaPrecios = FP.[IdAuxi], ' +
					N'@PIdReagListaPrecios	= FP.[IdReagAuxi], ' +
					N'@PIdListaPrecios	= FP.[IdCtaReagAuxi] From ' +
					@BasePhyDestino + N'.[dbo].FACListaPrecios FP inner join ' + 
					@BasePhyDestino + N'.dbo.ReagrupacionCuentasAuxi RC ' +
					N' On FP.IdPpal = RC.IdPpal and FP.IdAuxi = RC.IdAuxi and FP.IdReagAuxi = RC.IdReagAuxi and FP.IdCtaReagAuxi = RC.IdCtaReagAuxi' +
					N' Where DateDiff(day,@Pfecha,isnull(FP.FechaBaja,0)) < 0 ' +
					N'and RC.IdCtaAuxi = ''' + ltrim(rtrim(cast(@IdCtaAuxi as varchar))) + ''''
			Select	@param = N'@PIdAuxiListaPrecios smallint OUTPUT, ' +
								N'@PIdReagListaPrecios smallint OUTPUT, ' +
								N'@PIdListaPrecios char(12) OUTPUT, ' +
								N'@Pfecha datetime '
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Traigamos la lista de precios 1: ' + @sql					
			exec sp_executesql @sql, @param, @PIdAuxiListaPrecios = @IdAuxiListaPrecios OUTPUT,
								@PIdReagListaPrecios = @IdReagListaPrecios OUTPUT,
								@PIdListaPrecios = @IdListaPrecios OUTPUT,
								@Pfecha = @fecha
			-- Si la FechaBaja no esta cargada, la diferencia entre GetDate y cero es negativa
			-- Si la fechaBaja esta cargada y es previa a GetDate, tambien es negativo
			-- Tiene un mayor estricto, asi que si la FechaBaja es igual a hoy
			-- no se va a cumplir (o sea, toma la vigencia hasta las 0:00 de la FechaBaja)
			If @IdListaPrecios is null
			Begin
				-- Leo los valores de lista de precios por default
				-- si no hay ninguna reagrupacion que relacione el cliente con la lista de precios
				Select	@Sql = N'select	@PIdListaPrecios	= FP.IdListaPreciosDefault, ' +
						' @PIdReagListaPrecios = FP.IdReagListaPrecios,' +
						' @PIdAuxiListaPrecios = FP.IdAuxiClientes from	' +
						+ @BasePhyDestino + N'.[dbo].Facparametros FP inner join ' + 
							@BasePhyDestino + N'.[dbo].FACListaPrecios FLP ' +
							N' on FLP.IdPpal = FP.IdPpal and FLP.IdAuxi = FP.IdAuxiClientes ' +
							N' and FLP.IdReagAuxi = FP.IdReagListaPrecios and FLP.IdCtaReagAuxi = FP.IdListaPreciosDefault ' +
						N' Where DateDiff(day,@Pfecha,isnull(FLP.FechaBaja,0)) < 0'
				Select	@param = N'@PIdListaPrecios char(12) OUTPUT, ' +
								N'@PIdReagListaPrecios smallint OUTPUT, ' +
								N'@PIdAuxiListaPrecios smallint OUTPUT, ' +
								N'@Pfecha datetime'
				if @modoDebug > 0 and @modoDebug < 3
					Print 'Traigamos la lista de precios 2 (default, no habia asignada al cliente): ' + @sql					
				exec sp_executesql @sql, @param, @PIdListaPrecios = @IdListaPrecios OUTPUT,
									@PIdReagListaPrecios = @IdReagListaPrecios OUTPUT,
									@PIdAuxiListaPrecios = @IdAuxiListaPrecios OUTPUT, @Pfecha = @fecha
			End
			-- Si no hay tampoco una lista definida por default, queda en null
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Lista de precios detectada:' + isnull(cast(@IdListaPrecios as varchar),' NULA') + ' Reagrupacion de lista de precios:' + isnull(cast(@IdReagListaPrecios as varchar),' NULA')
			if (@IdListaPrecios is not null) -- solo proceso los productos si la lista no es nula
			begin
				Select	@sql =	N'Update	##ProductosBuscados ' +
								N'Set		PBprecioUnitario = P.precio From ' +
								@BasePhyDestino + N'.dbo.FACListaPrecios LP INNER JOIN ' +
								@BasePhyDestino + N'.dbo.FACPrecios P ON LP.IdPpal = P.IdPpal ' +
								N'	AND LP.IdAuxi = P.IdAuxi AND LP.IdReagAuxi = P.IdReagAuxiLP AND LP.IdCtaReagAuxi = P.IdCtaReagAuxiLP ' +
								N' WHERE LP.IdCtaReagAuxi = ''' + ltrim(rtrim(cast(@IdListaPrecios as varchar))) + '''' + 
								N' and P.IdProducto = ##ProductosBuscados.PBIdProducto collate Modern_Spanish_CI_AS ' +
								N' and P.IdPlanProducto = ''' + ltrim(rtrim(cast(@IdPlanProducto as varchar)))  + '''' +
								N' and P.vigencia = (select max(vigencia) Vigencia From ' +
								@BasePhyDestino + N'.dbo.FACListaPrecios LP INNER JOIN ' +
								@BasePhyDestino + N'.dbo.FACPrecios P ' +
								N'ON LP.IdPpal = P.IdPpal AND LP.IdAuxi = P.IdAuxi AND LP.IdReagAuxi = P.IdReagAuxiLP AND LP.IdCtaReagAuxi = P.IdCtaReagAuxiLP  ' + 
								N'WHERE LP.IdCtaReagAuxi = ''' + ltrim(rtrim(cast(@IdListaPrecios as varchar))) + '''' + 
								N' and P.IdPlanProducto = ''' + ltrim(rtrim(cast(@IdPlanProducto as varchar)))  + '''' +
								N'		and @Pfecha > vigencia ' +
								N' and P.IdProducto = ##ProductosBuscados.PBIdProducto collate Modern_Spanish_CI_AS ' +
								N' Group by P.idproducto ) '
				Select	@param = N'@Pfecha datetime'
				if @modoDebug > 0 and @modoDebug < 3
				Begin
					Print 'Busco los precios y los meto en la tabla temporal:'
					Print @sql				
				End
				exec sp_executesql @sql, @param, @fecha
				Update	##tempRemitosBIS
				Set		precioUnit = isnull(PBprecioUnitario,0),
						precio = isnull(PBprecioUnitario,0) * peso
				From	##ProductosBuscados
				Where	CodigoPhysis = PBIdproducto collate Modern_Spanish_CI_AS
				-- Si corresponde, tengo que manosear el IVA del precio
				-- 0: Precio no se usa
				-- 1: precio no se toca
				-- 2: Precio hay que agregar IVA
				-- 3: Precio hay que sacarle IVA
				-- Asi que solamente en los casos 2 y 3 hago algo
				If @MCcriterioPrecio in ('2','3')
				begin
					Select	@sql = N'Update	##tempRemitosBIS Set precio = cast(precio '
					Select	@sql = @sql + case when @MCcriterioPrecio = '2' then '*' else '/' end
					Select	@sql = @sql + N' (([Tasa]/100)+1) as decimal(10,5)) FROM ' +
						@BasePhyDestino + N'.[dbo].[FACTipoTasas] TT inner join ' +
						@BasePhyDestino + N'.[dbo].[FACProductos] P on p.idtasaiva = TT.idtipotasa where tt.[fechaBaja] is null ' +
						N' and p.idproducto = ##tempRemitosBIS.CodigoPhysis collate  Modern_Spanish_CI_AS'
					exec sp_executesql @sql
					if @modoDebug > 0 and @modoDebug < 3
					begin
						Print 'Ajuste de precios tocando IVA:'
						Print @sql
					end
				end
			end 
		End
		-- tengo los dos datos, el precio y el precioUnitario
		-- ATTENTI! tengo que cargar estas variables:
		--@PrecioUnitarioNeto
		--@precioUnitario -- igual a la anterior
		--y @PrecioNeto = @CantidadUMP *  @PrecioUnitario -- producto de precio * unidad UM
		-- Siquiera con ceros...
		 -- hasta aqui el tratamiento de precios de physis, que se hace solo si no fueron precios twins

		-- Ahora levanto los codigos de producto
		-- si no tiene ninguno cargado, asume 'UM'
		Select	@sql = N'Update ##ProductosBuscados ' +
						N'Set PBUnidadMedida = isnull(IdUMStock,''UM'') ' + 
						N'From ' + @BasePhyDestino + N'.dbo.facProductos ' +
						N'Where IdProducto = PBIdProducto collate Latin1_General_CI_AS'
		if @modoDebug > 0 and @modoDebug < 3
		Begin
			Print 'Update para cargar unidades de medida: '
			print @sql
		End
		exec sp_executesql	@sql
		/* *************************************************** 
		Hasta aca procesé un "detalle" completo. Tendria que meterlo en una
		estructura listo para mandar a Physis.
		Me conviene cargar todo el contenido del comprobante asi
		y finalmente insertarlo.
		 *************************************************** 
		 Meto todo tal cual lo voy a necesitar para physis en una nueva estructura (tabla variable)
			que incluya un numero de linea (autoincremento)
		 agregar una bandera de numero de ejecucion "segundaPasadaOMas"
		 al comenzar el ciclo del cursor que recorre la config me fijo en esa bandera, si esta activa
			entonces si el comprobante es distinto (revisar los campos phy), lo doy de alta
				acto seguido limpio la estructura de insercion physis
			si no es distinto, solo sigo cargando la estructura
		al salir del cursor tengo que hacer exactamente lo mismo, solo que sin mirar la bandera
			y directamente alta del comprobante
		 */
		-- Ahora combinando los datos recabados, las unidades de medida de la tabla auxiliar y
		-- el parametro movimientoNeg -util cuando
		Insert @DetallePhy (DPProducto,DPPartida, DPUM, DPCantidadUM,DPCantidadUMP,DPPrecioUnitario,
			DpPrecioUnitarioNeto,DPPrecioNeto,ObservacionesSTI)
			Select TR.CodigoPhysis,TR.Tropa, PB.PBUnidadMedida, 
				case @MCmovimientoNeg when 0 then TR.Unidades else (-1) * TR.Unidades end, 
				case @MCmovimientoNeg when 0 then TR.Peso else (-1) * TR.Peso end, 
				TR.precioUnit, 
				TR.precioUnit, 
				case @MCmovimientoNeg when 0 then (TR.precioUnit * TR.Peso) else ((-1) * TR.precioUnit * TR.Peso) end, 
				'Codigo ' + cast(TR.codigo as varchar) 
			From ##tempRemitosBIS TR inner join ##ProductosBuscados PB on PB.PBIdProducto = TR.CodigoPhysis
			Where TR.usuarioSinMatricula = case when @MCprocesaCCajena = 0 then 0 else TR.usuarioSinMatricula end
				and TR.usuarioSinMatricula <> case when @MCprocesaCCajena = 2 then 0 else 999999 end
				-- 27-05-2011 Edite precioUNit en lugar de precio arriba. JH
			-- me faltaria una condicion mas para que cuando MCprocesaCCajena=2 entonces tome solo los valores <> 0
			-- esta linea debe leerse asi: si esta configurado el proceso para tomar TODOS los movimientos, incluso
			-- los de usuarios sin matricula, la var @MCprocesaCCajena estará en 1. Si solamente corresponde procesar
			-- los de NC_CAL < 8, entonces estará en 0.
			-- El SP que carga la ##tempRemitosBIS (indirectamente) plancha los valores NC_CAL de 0 a 7 como cero
			-- Asi que si viene cero ahi y solo quiero los propios, cero = cero.
			-- Si en cambio quiero todo, campo=campo.
			-- Se supone que cuando cree la tabla productosBuscados ya resolvi los nulos, asi que aca no me preocupo	 
		-- Esto lo voy a usar en el alta, en el proximo ciclo o afuera
		Select	@ANTMCphyCompro	  = @MCphyCompro	
		Select	@ANTMCphyDeposito = @MCphyDeposito
		Select	@ANTMCSucursal	  = @MCSucursal
		Select	@ANTMCidProceso	  = @MCidProceso
		Select	@ANTMCphyTipoCompro= @MCphyTipoCompro 
		Select	@ANTMCprocesaCCajena= @MCprocesaCCajena 
		Select	@ANTMCphyPrecio		= @MCPhyPrecio
		Select	@ANTMCphyFechaTw	= @MCphyFechaTw
		Set		@ANTMCcumplidoPropio = @MCcumplidoPropio
		-- Listo, veamos el proximo...
		Fetch Next from MasterCursor into @MCidProceso,	@MCtwBaseCodPro, @MCtwBasePeso,	@MCtwBasePrecio, @MCphyPrecio, @MCcriterioPrecio,
				@MCphyBase,	@MCphyCompro, @MCphyDeposito, @MCSucursal, @MCphyTipoCompro, @MCmovimientoNeg, @MCprocesaCCajena, @MCphyFechaTw, @MCcumplidoPropio
	End
	close mastercursor
	deallocate mastercursor

	-- Termino el bucle principal

	if @ANTMCphyFechaTw	= 1 -- significa que la fecha que vale es la del comprobante en Twins
	begin	
		Select	@fecha = convert(smalldatetime,fecha)
		from dbo.[remitos resumen] 
		where nc_rem = (select top 1 CodigoRemitoTwins from @tempRemitos)
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Fecha y hora del comprobante: ' + cast(@fecha as varchar)
	end
	else -- sino, me quedo con solo la fecha
		Select @fecha = DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE()))
	-- Obtengo el ejercicio (usaré el mismo para todos y todas)
	-- Tomo el ejercicio de la fecha de ejecucion de la importacion
	Select	@Sql = N'Select @PidEjercicio = (select idEjercicio ' +
					N'	from ' + @BasePhyReferencia + N'.dbo.ejercicios ' +
					N'	where @Phoy between fechaInicio and fechaCierre)'
	Select	@Param	=	N'@Phoy	smalldatetime, @PidEjercicio smallint OUTPUT '
	exec sp_executesql @sql, @param, @fecha, @PidEjercicio = @idEjercicio output
	if @idEjercicio is null
	begin
		Select @ErrorMessage= 'No hay ningun ejercicio contable definido para la fecha del comprobante; NC_REM:' + cast(@twncrem as varchar)
		raiserror (@ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end
	if @modoDebug > 0 and @modoDebug < 3
	Begin
		Print @sql
		Print 'Parametro de fecha utilizado: ' + cast(@fecha as varchar)
		Print 'Ejercicio detectado: ' + cast(isnull(@idEjercicio,'NULO') as varchar)
	End
	-- voy a verificar que el comprobante exista (con su numerador) en Physis
	-- y si no esta, chau!
	--Select @sql = N'Select @PResul=1 from ' + @BasePhyDestino + '.dbo.TiposComprobante TC inner join ' +
	--@BasePhyDestino + '.dbo.NumeradoresPrefijos NP on NP.IdNumerador=TC.IdNumerador ' +
	--' where  TC.idtipocomprobante = ''' + ltrim(rtrim(@MCphyCompro)) + ''' collate  Modern_Spanish_CI_AS ' +
	--' and (replicate(''0'', 4 - len(NP.IdPrefijo)) + cast (NP.IdPrefijo as varchar)) = ''' + ltrim(rtrim(cast(@MCSucursal as varchar))) 
	--+ ''' collate  Modern_Spanish_CI_AS '
	--if @modoDebug < 2
	--	exec sp_executeSql @sql, N'@Presul int output', @Presul = @resul output
	--if @modoDebug > 0
	--	print @sql
	--if isnull(@resul,0)=0 -- significa que el comprobante no existe con ese numerador
	--begin
	--	select @ErrorMessage='El comprobante ' + @mcphycompro + ' con el punto de venta ' + @mcsucursal + ' no esta definido en Sifac'
	--	raiserror(@errormessage,16,1)
	--	if @@trancount > 0
	--		rollback tran
	--	return -1
	--end		
	-- y ya estaria para dar el alta al comprobante, su detalle y luego completo.
	-- 1ro borramos lo que haya para la conexion
	Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.spFACStock_Tmp_Delete ''' + cast(@IdConexion as nvarchar) + ''''
	if @modoDebug > 0
		print @sql
	if @modoDebug < 2
		exec sp_executesql @sql
	if @baseHija is not null
	begin
		Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
		if @modoDebug > 0
			print @sql
		if @modoDebug < 2
			exec sp_executesql @sql
	end
	if @modoDebug > 0 and @modoDebug < 3
		Print 'Otra vez, limpiamos conexiones phy (1.5)' 
	-- Uso un CURSOR para recorrer la tabla temporal creada y dar todo de alta
	Select @NroOrden = 0 -- inicio el renglon en cero
	---- DEBO: darle forma de sumatoria a la consulta

	DECLARE remitoCur cursor local For 
		SELECT 	distinct DPProducto, case DPPartida when '0' then null else cast(DPPartida as varchar) end,DPUM,DPCantidadUM,DPCantidadUMP,
				DPPrecioUnitario,DpPrecioUnitarioNeto,DPPrecioNeto,ObservacionesSTI		
		FROM 	@DetallePhy
	OPEN remitoCur
	FETCH NEXT FROM remitoCur
		INTO @Producto, @Partida, @UM, @CantidadUM, @CantidadUMP, @precioUnitario,
				@PrecioUnitarioNeto,@precioNeto, @ObservacionesSTI
	WHILE @@FETCH_STATUS = 0
	BEGIN
		Select @NroOrden = @NroOrden + 1 -- incremento el renglon (empieza en cero)
		/********* Ahora voy a leer la unidad de stock del producto en cuestion para utilizarla *****/
		if @partida='0'
			Select @partida = Null
		-- tengo que dejarla NULA para que no la cargue cuando se trata de cajas
		-- aunque hasta este punto considere como '0' las cajas
		-- porque NULLs quedaron las tropas no encontradas (de colgado), que tuve que reemplazar por la generica
		-- ahora si, vamo' pa' delante...
		Select @FacIdCabecera=NULL,@FacIdMovimiento=NULL
		if (@ANTMCphyTipoCompro = 'P' or @ANTMCphyTipoCompro='F')
			Select @FacClase = 'REM',
					@TipoFactura = 'REM'
			else
				Select @FacClase = '',
					@TipoFactura = ''
		if @ANTMCphyTipoCompro='R'
			Select @FacCantidad=NULL
		else
			Select @FacCantidad= case when @CantidadUMP=0 then @cantidadUM else @cantidadUMP end
		if @UM is null
		begin
			Print 'SD. ERROR GRAVE. La unidad de medida (UM) del producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definida'
			if @@trancount > 0
				rollback tran
			Select @ErrorMessage = 'SD. ERROR GRAVE. La unidad de medida (UM) del producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definida'
			raiserror (@ErrorMessage,16,1)
			return -1
		end
		if @Producto is null
		begin
			Print 'SD. ERROR GRAVE. El codigo de producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definido'
			if @@trancount > 0
				rollback tran
			Select @ErrorMessage =  'SD. ERROR GRAVE. El codigo de producto ' + cast(isnull(@Producto,'NULO') as varchar) + ' no esta definido'
			raiserror (@ErrorMessage,16,1)
			return -1
		end
		Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.SpFACStock_Tmp_Insert ' +
			ISNULL('''' + cast(@IdMovimiento as varchar) + '''',' NULL ') + ', ' +								
			'''' + cast(@NroOrden as varchar) + '''' + ', ' +
			'''' + ltrim(rtrim(cast(@Producto as varchar))) + '''' + ', ' +
			ISNULL('''' + cast(@IdAuxiPropietario as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@IdCtaAuxiPropietario as varchar) + '''',' NULL ') + ', ' +												--
			ISNULL('''' + ltrim(rtrim(cast(@Partida as varchar))) + '''',' NULL ') + ', ' +								
			'''' + cast(@UM as varchar) + '''' + ', ' +
			'''' + cast(@CantidadUM as varchar) + '''' + ', ' +
			'''' + cast(@CantidadUMP as varchar) + '''' + ', ' +
			quotename(isnull(@PrecioUnitario,0))  + ', ' +
			quotename(isnull(@Descuento ,0))  + ', ' +
			quotename(isnull(@PrecioUnitarioNeto,0))  + ', ' +
			quotename(isnull(@PrecioNeto,0))  + ', ' +
			quotename(isnull(@ImpuestosInternos,0)) + ','+
			'''' + cast(@FechaVencimiento as varchar) + '''' + ', ' +
			'''' + @ObservacionesSTI + '''' + ', ' +
			'''' + cast(@AcumulaProducto as varchar) + '''' + ', ' +
			ISNULL('''' + cast(@PedIdCabecera as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@PedIdMovimiento as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@PedCantidad as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@RemIdCabecera as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@RemIdMovimiento as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@RemCantidad as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@FacIdCabecera as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@FacIdMovimiento as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@FacCantidad as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@IdLiquidoProducto as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@IdCabeceraViaje as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@IdMovimientoViaje as varchar) + '''',' NULL ') + ', ' +								
			'''' + cast(@IdConexion as varchar) + '''' + ', ' +
			'''' + @FacClase  + '''' + ', ' +
			ISNULL('''' + cast(@ProductoConjunto as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@NivelConjunto as varchar) + '''',' NULL ') + ', ' +								
			'''' + cast(@IdPlanProducto as varchar) + '''' + ', ' +
			'''' + cast(@RecuperoKgLimpio as varchar) + '''' + ', ' +
			'''' + rtrim(ltrim(@MCphyDeposito)) + '''' + ', ' +
			'''' + cast(@CantidadUMRemesa as varchar) + '''' + ', ' +
			'''' + cast(@CantidadUMDif as varchar) + '''' + ', ' +
			'''' + cast(@CantidadUMPorc as varchar) + '''' + ', ' +
			ISNULL('''' + cast(@CantidadUMPRemesa as varchar) + '''',' NULL ') + ', ' +
			ISNULL('''' + cast(@CantidadUMPDif as varchar) + '''',' NULL ') + ', ' +
			ISNULL('''' + cast(@CantidadUMPPorc as varchar) + '''',' NULL ') + ', ' +
			ISNULL('''' + cast(@CodCampo as varchar) + '''',' NULL ') + ', ' +								
			ISNULL('''' + cast(@CodLote as varchar) + '''',' NULL ')  
		if @modoDebug > 0 and @modoDebug < 3
			print 'Alta de renglon de detalle en comprobante (DOS)'
		if @modoDebug > 0 
			Print isnull(@sql,'NULO')
		if @modoDebug < 2
		begin try
			exec sp_executesql @sql--, @param, @IdConexion
		end try
		begin catch
			print 'Error grave (Stock - conso 1): '
			print ERROR_NUMBER() 
			print ERROR_SEVERITY() 
			print ERROR_STATE() 
			print ERROR_PROCEDURE() 
			print ERROR_LINE() 
			print ERROR_MESSAGE() 
			if @@trancount > 0
				ROLLBACK tran 
			Select @ErrorMessage='Error al tratar de procesar por interfaz. '+ ERROR_MESSAGE() 
			raiserror (@ErrorMessage,16,1)
			return -1
		end catch
		If @BaseHija is not null
		begin
			Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
			if @modoDebug > 0 and @modoDebug < 3
				Print 'Hija: ' + isnull(@sql,'NULO') -- el de la hija
			if @modoDebug > 0
				print @sql
			if @modoDebug < 2
				begin try
					exec sp_executesql @sql--, @param, @IdConexion
				end try
				begin catch
					print 'Error grave (Stock - hija 2): '
					print ERROR_NUMBER() 
					print ERROR_SEVERITY() 
					print ERROR_STATE() 
					print ERROR_PROCEDURE() 
					print ERROR_LINE() 
					print ERROR_MESSAGE() 
					if @@trancount > 0
						ROLLBACK tran 	
					Select @ErrorMessage='Error al tratar de procesar por interfaz. '+ ERROR_MESSAGE() 
					raiserror (@ErrorMessage,16,1)
					return -1
				end catch
		end
		else
			if @mododebug > 0
				Print isnull(@BaseHija,'Base hija NULA')
		if (@ANTMCphyTipoCompro = 'F' or @ANTMCphyTipoCompro='P') and isnull(@partida,'0') <> '0' and 1=0
		begin
			-- Verifico si ya lo habia cargado, si hay mas productos de la misma partida/tropa, lo estar duplicando
			Select @sql='Select @PResul=count(1) from ' + @BasePhyDestino + N'.dbo.FACStockAuxiliares_Tmp where Idctaauxiliar = ''' + ltrim(rtrim(cast(@Partida as varchar))) +
				''' and conexion = ''' + ltrim(rtrim(cast(@IdConexion as varchar))) + ''' and idplanauxiliar= ''' + ltrim(rtrim(cast(@ConstIdAuxiCentroCostos as varchar))) + ''''
			Select @Param = '@PResul int output'
			exec sp_executesql @sql, @param, @Presul = @resul output
			if isnull(@resul,0)=0
			begin
				Select @IdMovimiento = 0 --(Solo tiene si estoy editando en el alta van en 0)
				Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.spFACStockAuxiliaresTMP_Insert ' +
				ISNULL('''' + cast(@IdCabecera as varchar) + '''',' NULL ') + ', ' +								
				ISNULL('''' + cast(@IdMovimiento as varchar) + '''',' NULL ') + ', ' +								
				'''' + cast(@NroOrden as varchar) + '''' + ', ' +
				'''' + cast(@ConstIdAuxiCentroCostos as varchar) + '''' + ', ' +
				ISNULL('''' + ltrim(rtrim(cast(@Partida as varchar))) + '''',' NULL ') + ', ' +	--@IdCtaAuxiliar Numero de cuenta (en este caso es el mismo Nro de la Tropa)
				'''' + cast(@IdConexion as varchar) + '''' 
				/*
				spFACStockAuxiliaresTMP_Insert     (@IdCabecera int, @IdMovimiento smallint, @NroOrden Tinyint, 
						@IdPlanAuxiliar smallint, @IdCtaAuxiliar varchar(12), @Conexion int)

				donde :
				@IdCabecera, @IdMovimiento (Solo tiene si estoy editando en el alta van en 0)
				@NroOrden  en la grilla. 
				@IdPlanAuxiliar Id del plan de Centros de Costo
				@IdCtaAuxiliar Numero de cuenta (en este caso es el mismo Nro de la Tropa)
				@Conexion 
				*/
				if @modoDebug > 0 and @modoDebug < 3
					print 'Alta de renglon de detalle para centro de costos (DOS)'
				if @modoDebug > 0
					Print @sql
				if @modoDebug < 2
					exec sp_executesql @sql
				If @BaseHija is not null
				begin
					Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
					if @modoDebug < 2
						exec sp_executesql @sql
					if @modoDebug > 0
						print @sql
				end
			end
		end

		FETCH NEXT FROM remitoCur
			INTO @Producto, @Partida, @UM, @CantidadUM, @CantidadUMP, @precioUnitario,
				@PrecioUnitarioNeto,@precioNeto, @ObservacionesSTI
	END	
	-- ahora el alta definitiva del comprobante
	close		remitoCur
	Deallocate	remitoCur
	-- cuando esto termina, me quedan igual un par de campos cargados
	-- @ObservacionesSIUP, @cuit
	-- antes que nada obtengo el total con IVA 13/06/2014
	-- 14/04/2015. Me falto contemplar en el calculo del total y del iva total, que cuando
	-- el comprobante tiene mercaderia ajena, puede que no deba incluirse en el total
	-- asi que agrego las condiciones con MCprocesaCCAjena para contemplarlo

				


	Select	@sql = 'select @pPrecio = sum(tr.Precio * (([Tasa]/100)+1)), @pTotalIVA = sum(tr.Precio * (([Tasa]/100))), @pTotalNetoGravado = sum(tr.Precio) ' 
		+ N'FROM ##tempRemitosBIS TR inner join ' +
		@BasePhyDestino + N'.dbo.[FACProductos] P on p.idproducto = TR.CodigoPhysis collate  Modern_Spanish_CI_AS inner join ' +
		@BasePhyDestino + N'.dbo.[FACTipoTasas] TT on p.idtasaiva = TT.idtipotasa ' +
		'where tt.[fechaBaja] is null ' + 
		' and TR.usuarioSinMatricula = case when ' + cast(@ANTMCprocesaCCajena as varchar) + ' = 0 then 0 else TR.usuarioSinMatricula end ' +
		'		and TR.usuarioSinMatricula <> case when ' + cast(@ANTMCprocesaCCajena as varchar) + ' = 2 then 0 else 999999 end'
	Select @param = N'@pPrecio money output, @pTotalIVA money output, @pTotalNetoGravado money output'
	exec sp_executesql @sql, @param, @pPrecio = @TotalFactura output, @pTotalIVA = @TotalIVA output, @pTotalNetoGravado = @TotalNetoGravado output
	Select @TotalNeto = @totalNetoGravado
	--if @modoDebug > 0 and @modoDebug < 3
		--Print 'Total bruto: ' + cast(@TotalNetoGravado as varchar) + '; Total IVA: ' + cast(@TotalIVA as varchar) + '; Total con impuestos: ' + cast(@totalFactura as varchar)
				-- Si el comprobante NO LLEVA IVA tengo que poner el IVA en cero e igualar el total al totalnetogravado
	If @BaseHija is not null
	begin
		Select @Sql = N'select top 1 @Presul=nocalculaautomatico from ' + @baseHija + '.dbo.empresa'
		if @modoDebug > 0 and @modoDebug < 3
			print 'Determino si plancho el IVA: ' + isnull(@sql,'NULO')
		exec sp_executesql @sql, N'@Presul int output', @Presul=@resul output
		if isnull(@resul,0)=1
		begin
			if @modoDebug > 0 and @modoDebug < 3
				Print 'iva planchado'						
			Select @TotalIVA = 0
			Select @TotalFactura = @TotalNetoGravado
		end
		else
			if @modoDebug > 0 and @modoDebug < 3
				Print 'iva sin tocar'						
	end
	else
		if @modoDebug > 0
			Print 'No hice nada con el IVA porque no hay base hija definida'
	--if @modoDebug > 0 and @modoDebug < 3
	--	Print 'Total del comprobante (2): ' + cast(isnull(@totalfactura,'NULO') as varchar)
	select @ObservacionesSIUP = (
					SELECT top 1 'Codigo interno twins (NC_REM) nro ' + cast(CodigoRemitoTwins as varchar) + ' y codigo de carga ' + cast(rr.nro_carga as varchar)
				from @tempRemitos TR inner join dbo.[remitos resumen] RR on TR.CodigoremitoTwins = RR.nc_rem )
	-- la consulta que trae los datos del cliente (uno x uno) usa el CUIT como clave
	-- y no toma en cuenta los que se hayan dado de baja (en physis, obviamente)
	-- ademas se queda con el nro de cuenta más grande (por si hay alguno de mas)
	select @IdCtaAuxi=  @codigoCliente
	-- la @IdCtaAuxi la veo mas arriba...
	-- Ahora traigo los datos del cliente, a lo dinamico
	Select @sql = N'Select top 1 @PIdTipoDocumento = t.IdTipoDocumento, @PNumeroDocumento = t.NumeroDocumento, ' +
		N'@PCategoriaIVA = t.CategoriaIVA From ' + @BasePhyDestino + N'.dbo.terceros t ' +
		N'where t.idctaAuxi = @PcodigoCliente and t.IdPpal = @PConstIdPpal and t.IdAuxi = @PConstIdAuxi '
	Select @param = N'@PcodigoCliente varchar(12), @PConstIdPpal smallint, @PConstIdAuxi smallint, ' +
					N'@PIdTipoDocumento varchar(5) OUTPUT, @PNumeroDocumento varchar(12) OUTPUT, ' +
					N'@PCategoriaIVA varchar(2) OUTPUT'
	exec sp_executesql @sql, @param, @codigoCliente, @ConstIdPpal, @ConstIdAuxi, @PidTipoDocumento = @idTipoDocumento OUTPUT, 
		@PNumeroDocumento = @NumeroDocumento OUTPUT, @PCategoriaIva = @CategoriaIVA OUTPUT
	if @modoDebug > 0 and @modoDebug < 3
	Begin
		Print 'Busqueda de datos del cliente, parte 1:'
		print @sql
	End
	-- Ahora el nombre del tercero:
	Select @sql = N'Select top 1 @PNombreTercero=c.Nombre From ' + @BasePhyDestino +
		N'.dbo.cuentasAuxi c where c.idctaAuxi = @PcodigoCliente and c.IdPpal = @PConstIdPpal and	c.IdAuxi = @PConstIdAuxi'
	Select @param = N'@PcodigoCliente varchar(12), @PConstIdAuxi smallint, @PConstIdPpal smallint, @PNombreTercero varchar(40) OUTPUT' 
	exec sp_executesql @sql, @param, @CodigoCliente, @ConstIdAuxi, @ConstIdPpal, @PNombreTercero = @NombreTercero OUTPUT
	if @modoDebug > 0 and @modoDebug < 3
	Begin
		Print 'Busqueda de datos del cliente, parte 2:'
		Print 'Uso el codigo de cliente ' + CAST(ISNULL(@codigoCliente,'NULLLOOO') as varchar)
		print @sql
	End
	-- cuando se trata de un PRESUPUESTO tengo que diferenciarlo entre A y B
	-- como es el caso tambien con las facturas
	If (@ANTMCphyTipoCompro = 'P' or @ANTMCphyTipoCompro='F')	-- sobreescribo @ANDMCphyCompro segun corresponda A o B
		if (@CategoriaIva = '01' or @CategoriaIVA = '10' or @CategoriaIVA = '11')
			Select @ANTMCphyCompro = rtrim(ltrim(@ANTMCphyCompro)) + 'A'
		else 
			Select @ANTMCphyCompro = rtrim(ltrim(@ANTMCphyCompro)) + 'B'
	/*
	-- para la movida consolidada-hijas tengo que hacer una jugada distinta con el IdCabecera
	-- primero lo obtengo con un SP especifico de la consolidada y luego lo paso como parametro
	-- al resto de la familia
	Select	@sql = N'exec ' + @BasePhyDestino + N'.dbo.SpFACComprobantes_SelectNext' + 
	'''' + cast(@IdEjercicio as varchar) + '''' + ', ' +
	'''' + @ANTMCphyCompro + '''' + ', ' +
	'''' + @Numero + '''' 
	--exec sp_executesql @sql
	--devuelve tres variables: ReplIdCabecera, ReplIdComprobante, ReplNumero
	--	P/e: Numero=0000020 IdCabecera=1998 IdComprobante=2302
	--Declare @ComprobanteConso table (NumeroConsolidado varchar(12), IdCabeceraConsolidado Int, IdComproConsolidado Int)
	Delete from @ComprobanteConso
	insert @ComprobanteConso
	exec (@sql)
	if @modoDebug > 0 
		print @sql
	-- finalmente con esto cargo el nro de cabecera que va "replicado" a las dos DB: (los otros dos valores no los uso)
	Select @IdCabeceraRepl = (select top 1 IdCabeceraConsolidado from @ComprobanteConso)
	*/
	-- REMITO
	If (@ANTMCphyTipoCompro = 'P') or (@ANTMCphyTipoCompro = 'F')-- solo ejecuto esto en caso de presupuestos
	Begin
		Select	@sql = 'exec ' + @BasePhyDestino + N'.dbo.spFillFACVencimiento_Manual_tmp ' +
			'''' + cast(@ConstIdPpal as varchar) + '''' + ', ' + 
			'''' + cast(@IdAuxi as varchar) + '''' + ', ' + 
			'''' + cast(@IdReagCondPago as varchar) + '''' + ', ' + 
			'''' + cast(@IdCondPago as varchar) + '''' + ', ' + 
			'''' + cast(@IdConexion as varchar)+ '''' 
		if @modoDebug > 0 and @modoDebug < 3
			Print 'Llamada a FillFACVencimiento_Manual_tmp ' + isnull(@sql,'NULA, changos')
		if @modoDebug > 0
			print @sql
		if @modoDebug < 2
			exec sp_executesql @sql
		if @baseHija is not null
		begin
			Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
			if @modoDebug < 2
				exec sp_executesql @sql
			if @modoDebug > 0
				print @sql
		end
	End 
	--exec spFillFACVencimiento_Manual_tmp 1, @IdAuxi, @IdReagCPago, @IdCondPago, @IdConexion 
	Select @IdTipoComprobanteExt = @ANTMCphyCompro	-- comprobante externo, es el mismo
	Select @Numero = cast(@ANTMCSucursal as varchar) + CAST(@numero as varchar)
/*	Select @IdTipoComprobanteExt = @ANTMCphyCompro	-- comprobante externo, es el mismo
	Select @Numero = cast(@ANTMCSucursal as varchar) + CAST(@numero as varchar)
	Select	@Sql = N'exec ' + @BasePhyDestino + N'.dbo.SpFACStock_Insert_Update_Rem ' +
*/
	if (@IdCtaAuxi is null) or (@IdTipoDocumento is null) or (@NumeroDocumento is null)
	BEGIN
		select @ErrorMessage = '2. Falta un dato (nulo): ' 
		if (@idctaauxi is null) select @ErrorMessage = @ErrorMessage + 'IdCtaAuxi'
		if (@IdTipoDocumento is null) select @ErrorMessage = @ErrorMessage + ' IdTipoDocumento de la cuenta ' + cast(@idctaauxi as varchar)
		if (@NumeroDocumento is null) select @ErrorMessage = @ErrorMessage + ' NumeroDocumento de la cuenta ' + cast(@idctaauxi as varchar)
		if @@trancount > 0
			rollback tran
		Select @ErrorMessage = @ErrorMessage + ' NC_REM: ' + cast(@twncrem as varchar)
		RAISERROR(@ErrorMessage,16,1)
		return -1				
	end	
	Select @sql = 'exec ' + @BasePhyDestino + 
			case when @ANTMCphyTipoCompro='R' then N'.dbo.SpFACStock_Insert_Update_Rem '
				when @ANTMCphyTipoCompro='P' then N'.dbo.SpFACStock_Insert_Update_Fac '
				when @ANTMCphyTipoCompro='F' then N'.dbo.SpFACStock_Insert_Update_Fac '
				when @ANTMCphyTipoCompro='D' then N'.dbo.SpFACStock_Insert_Update_Ped '
			end +
		'''' + @ABMD + '''' + ', ' +
		'''' + cast(@IdCabecera as varchar) + '''' + ', ' +
		'''' + cast(isnull(@IdEjercicio,'NULO') as varchar) + '''' + ', ' +
		'''' + @ANTMCSucursal+ '''' + ', ' +
		'''' + CONVERT(nvarchar(30), @fecha, 126) + '''' + ', ' +
		'''' + ltrim(rtrim(@ANTMCphyCompro)) + '''' + ', '  +
		'''' + cast(@Numero as varchar) + '''' + ', ' + -- pto venta + nro
		'''' + cast(@IdAuxi as varchar) + '''' + ', ' +
		'''' + @IdCtaAuxi + '''' + ', ' +
		'''' + ltrim(rtrim(@IdTipoDocumento)) + '''' + ', ' +
		'''' + @NumeroDocumento + '''' + ', '  +
		case when @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P'
			then '''' + @TipoFactura + '''' + ', ' else '' end +
		'''' + @NombreTercero + '''' + ', ' +
		'''' + @CategoriaIVA + '''' + ', ' +
		'''' + @ObservacionesSIUP + '''' + ', ' +
		'''' + ltrim(rtrim(@ANTMCphyDeposito)) + '''' + ', ' +				
		case when @ANTMCphyTipoCompro='R' then 'NULL,'		-- Deposito "A" (o sea, remito para meter en otro deposito)
			else '' end +
		ISNULL('''' + cast(@IdAuxiListaPrecios as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdReagListaPrecios as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdListaPrecios as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdReagVendedor as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdVendedor as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdReagTransporte as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdTransporte as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdReagDescuento as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdDescuento1 as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@Descuento1 as varchar) + '''',' NULL ') + ', ' +				
		ISNULL('''' + cast(@IdDescuento2 as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@Descuento2 as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@IdReagObservaciones as varchar) + '''',' NULL ') + ', ' +								
		ISNULL('''' + cast(@IdCodObservaciones as varchar) + '''',' NULL ') + ', ' +
		'''' + ltrim(rtrim(@Referencia)) + '''' + ', ' + 
		ISNULL('''' + cast(@IdReagCondPago as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + ltrim(rtrim(cast(@IdCondPago as varchar))) + '''',' NULL ') + ', '  +		
		case when  @ANTMCphyTipoCompro='D' then ISNULL('''' + cast(@TotalNeto as varchar) + '''','0') 
			else  ISNULL('''' + cast(@FormaCosteo as varchar) + '''',' NULL ') 
		end + ', ' +		
		'''' + cast(@Alcance as varchar) + '''' + ', ' +
		'''' + cast(@ModoCarga as varchar) + '''' + ', ' +
		ISNULL('''' + cast(@IdMoneda as varchar) + '''',' NULL ') + ', ' +								
		ISNULL('''' + cast(@Serie as varchar) + '''',' NULL ') + ', ' +
		ISNULL('''' + cast(@TasaCambio as varchar) + '''',' NULL ') + ', '
		If @ANTMCphyTipoCompro='R' 
			Select @sql = @sql + isnull('''' + cast(isnull(@TotalNetoGravado,0) as varchar) + '''',' NULL ') + ', '
		If @ANTMCphyTipoCompro='D' 
			Select @sql = @sql + ISNULL('''' + cast(@GrabarViaje as varchar) + '''','0') + ', '
		Select @sql = @sql +
			ISNULL('''' + cast(@IdUsuario as varchar) + '''',' NULL ') + ', ' +
			ISNULL('''' + cast(@IdConexion as varchar) + '''',' NULL ') + ', ' 
		-- Esto solo para FACturas y PResupuestos
		If @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P'
		begin
			--Select @sql = @sql + --'0,0,0,0,0,1,'
			--cast(@totalFactura as varchar)  + ', 0,0,0,' +
			--cast(@totalFactura as varchar)  + ', 1,'
			Select @sql = @sql +
				ISNULL('''' + cast(@TotalNeto as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@TotalIVA as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@TotalIVARNI as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@TotalPercepcionIVA as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@TotalFactura as varchar) + '''',' NULL ') + ',1, ' -- el ultimo es @Definitiva 
			--	@Definitiva         bit,      falta totalnetogravado y totalnetonogravado                  
					
		end 
		If @ANTMCphyTipoCompro='R'
		begin
			Select @sql = @sql + 
				ISNULL('''' + cast(@CodCampania as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@Planta as varchar) + '''',' NULL ') + ', ' 
		end
		If @ANTMCphyTipoCompro='D'
			Select @sql = @sql + 
					ISNULL('''' + cast(@forTranferWinsifac as varchar) + '''','0') + ', ' 
		else
			Select @sql = @sql + 
				ISNULL('''' + cast(@FechaExt as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@IdTipoComprobanteExt as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@NumeroExt as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@FechaVencimientoCAI as varchar) + '''',' NULL ') + ', ' +
				case when @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
					then '''' + '' + '''' + ', ' else '' end +		-- @NumeroCAI varchar(14)
				ISNULL('''' + cast(@IdPais as varchar) + '''',' NULL ') + ', ' +								
				ISNULL('''' + cast(@IdProvincia as varchar) + '''',' NULL ') + ', ' 
		if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
			Select @sql = @sql + '0,NULL,NULL,NULL,NULL,'''',0,'
		/*
			@GrabaNegativos	    bit,                   
			@CodCampania  	    smallint = Null ,         
			@IdMonedaPrint      Char(5) = Null,        
			@SeriePrint         TinyInt = Null,        
			@TasaPrint          float = Null,         
			@MensajeError       varchar(1000) output,      
			@EsMerma		bit = 0, 	            
		*/
		Select @sql = @sql + 'IDCABECERAREPL'
		If @ANTMCphyTipoCompro='D'
			Select @sql = @sql + ISNULL(''',' + cast(@CodCampania as varchar) + '''',', NULL ') + ', ' +
				ISNULL('''' + cast(@IdEstado as varchar) + '''',' NULL ') 

		--	ISNULL('''' + cast(@IdCabeceraRepl as varchar) + '''',' NULL ')		
		/* Si es factura o proforma todavia falta esto:
			@IdComprobanteSigesRepl 	int = 0,  
			@FElectronica 	  	bit = 0,      
			@FENroSolicitud 		int = 0,  
			@FEEsServicio 		smallint = 1,    
			@FEServicioFechaDesde 	DateTime = NULL,    
			@FEServicioFechaHasta 	DateTime = NULL,    
			@FERespuestaAFIP 		Varchar(500) = NULL, 
			@TotalNetoGravado		money = 0,           
			@TotalNetoNoGravado		money = 0, 
			@FechaIVA				datetime = Null, 
			@IdIdioma				Int = Null, 
			@MultiCuentaDeudor      bit = 0 
		*/
		if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
			Select @sql = @sql + ',IDCOMPROBANTESIGESREPL,0,0,1,NULL,NULL,NULL,' +
				ISNULL('''' + cast(@totalNetoGravado as varchar) + '''',' NULL ') + ', ' +
				ISNULL('''' + cast(@TotalNetoNoGravado as varchar) + '''',' NULL ') + ',NULL,NULL,0'

	if @modoDebug > 0 and @modoDebug < 3
		Print 'Alta definitiva del comprobante - base consolidada:'
	Select @sqlProvi =  replace(@sql,'IDCABECERAREPL','0')
	Select @sqlProvi =  replace(@sqlProvi,'IDCOMPROBANTESIGESREPL','0')
	if @modoDebug > 0
		print @sqlProvi
	begin try
		delete from #CabecerasDevueltas
		if (@ANTMCphyTipoCompro='R' or @ANTMCphyTipoCompro='D') and exists (SELECT 1 FROM TempDB.INFORMATION_SCHEMA.COLUMNS
				where table_name like '#cabecerasdevueltas%' and column_name=N'idcomprobante')
			alter table #CabecerasDevueltas  
				drop column idcomprobante	--NumeroDefinitivo varchar(20) null, cabecera int null, idcomprobante varchar(20) null)
		if (@ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P') and 	not exists (SELECT 1 FROM TempDB.INFORMATION_SCHEMA.COLUMNS
				where table_name like '#cabecerasdevueltas%' and column_name=N'idcomprobante')
			alter table #CabecerasDevueltas  
				add idcomprobante varchar(20)
	end try
	begin catch
		Print 'Error grave antes de ejecutar el alta (cabecera - conso 2):'
		SELECT 
			@ErrorMessage = 'Error grave antes de ejecutar el alta (cabecera - conso 2)' + ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, -- Message text.
				   @ErrorSeverity, -- Severity.
				   @ErrorState -- State.
				   );
		if @@trancount > 0 -- dentro de un catch no funciona
			ROLLBACK tran 	
		return -1
	end catch

	if @modoDebug < 2
	begin
		delete from #CabecerasDevueltas
		insert #CabecerasDevueltas
			exec sp_executesql @sqlProvi
		if @@error <> 0
		begin
			if @@trancount > 0
				rollback tran
			Select @ErrorMessage = 'Error al dar de alta comprobante en Physis. NC_REM: ' + cast(@twncrem as varchar)
			raiserror(@errormessage,16,1)
			return -1
		end
	end
	-- tomo la Idcabecera asignada en la consolidada, para usar en la hija
	select @IdCabeceraRepl = isnull((select top 1 cabecera from #CabecerasDevueltas),0)
	-- lo mismo con el IdComprobante de Siges
	if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
		Select @IdComprobanteSigesRepl = isnull((select top 1 idComprobante from #CabecerasDevueltas),0)
	if @modoDebug > 0 and @modoDebug < 3 
	begin
		print 'IdCabecera de replica: ' + cast (@IdCabeceraRepl as varchar)
		print 'IDComprobante de replica: ' + cast (isnull(@IdComprobanteSigesRepl,0) as varchar)
	end
	Select @sql = replace(@sql,'IDCABECERAREPL',cast(@IdCabeceraRepl as varchar) )
	if @ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P' 
		Select @sql = replace(@sql,'IDCOMPROBANTESIGESREPL',cast(@IdComprobanteSigesRepl as varchar) )

	-- si el comprobante es un Remito y esta configurado para dar por cumplidos los propios
	-- en este punto proceso la base consolidada
	if @ANTMCphyTipoCompro = 'R' and @ANTMCcumplidoPropio = 1
	begin
		-- comparo CUIT de cliente y propio
		-- doy por cumplido si son iguales
		print 'Aqui daria por cumplido la cabecera ' + cast(@IdCabeceraRepl as varchar)
		print @NumeroDocumento

		Select	@Sql2	= N'SELECT top 1 @PMiCuit = NumeroDocumento from ' + @BasePhyDestino + N'.dbo.Empresa'
		Select	@param2	= N'@PMiCuit varchar(12) OUTPUT'
		Print 'B.' + @Sql2
		if @modoDebug > 0 and @modoDebug < 3
			Print @Sql2
		exec sp_executesql @sql2, @param2, @PMiCuit = @MiCuit Output
		Print @Micuit
		If @NumeroDocumento = @MiCuit
		begin
			Print 'Se da por cumplido la cabecera' + cast(@IdCabeceraRepl as varchar)
			Select @sql2 = N'Update ' + @BasePhyDestino + N'.dbo.FacStock Set estado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
			Set @sql2 = @sql2 + '; Update ' + @BasePhyDestino + N'.dbo.FacCabeceras Set Idestado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
			set @sql2 = @sql2 + '; INSERT INTO ' + @BasePhyDestino + N'.dbo.FACCabecerasEstados (IdCabecera, IdEstado, FechaHora, IdUsuario) '
				+N'VALUES (' + cast(@IdCabeceraRepl as varchar) +', 2, getdate(), ' + cast(@IdUsuario as varchar)  +') '

			if @modoDebug > 0 and @modoDebug < 3
				Print @Sql2
			print @sql2
			exec sp_executesql @sql2
		end
		else
			Print 'NO se da por cumplido la cabecera' + cast(@IdCabeceraRepl as varchar)
		-- para evitar tooooda esta saraza en la hija podria usar el valor de MiCUIT como flag para indicar 
		-- que hay que dar por cumplido

	end


	/* If a distributed transaction executes within the scope of a TRY block and an error occurs, execution is transferred to the associated 
	CATCH block. The distributed transaction enters an uncommittable state. Execution within the CATCH block may be interrupted by the 
	Microsoft Distributed Transaction Coordinator which manages distributed transactions. When the error occurs, MS DTC asynchronously 
	notifies all servers participating in the distributed transaction, and terminates all tasks involved in the distributed transaction. 
	This notification is sent in the form of an attention, which is not handled by a TRY…CATCH construct, and the batch is ended. 
	When a batch finishes running, the Database Engine rolls back any active uncommittable transactions. If no error message was sent 
	when the transaction entered an uncommittable state, when the batch finishes, an error message will be sent to the client application 
	that indicates an uncommittable transaction was detected and rolled back*/		
	-- si la cabecera es cero o menor, algo fallo al dar de alta en la consolidada
	-- si el comprobante es factura o proforma, ademas verifico si el comprobantesSiges es valido
	if @IdCabeceraRepl <= 0 or ((@ANTMCphyTipoCompro='F' or @ANTMCphyTipoCompro='P') and @IdComprobanteSigesRepl <=0)
		and @modoDebug < 2
	begin
		Select @ErrorMessage = 'Error al tratar de procesar por interfaz.' + cast(@twncrem as varchar)
		raiserror (@ErrorMessage,16,1)
		if @@trancount > 0
			ROLLBACK tran 	
		return -1
	end
	if @baseHija is not null
	begin
		Select @sql = replace(@sql, @BasePhyDestino, @BaseHija)
		if @modoDebug > 0 and @mododebug < 3
			print 'Alta en base hija:'
		if @modoDebug > 0
			print @sql
		if @modoDebug < 2
		begin try
			exec sp_executesql @sql

			-- si el comprobante es un Remito y esta configurado para dar por cumplidos los propios
			-- en este punto proceso la base consolidada
					-- Repite lo del cumplimiento en la hija
			if (@ANTMCphyTipoCompro = 'R' and @ANTMCcumplidoPropio = 1) and (@NumeroDocumento = @MiCuit)
			begin
				Print 'Se da por cumplido la cabecera' + cast(@IdCabeceraRepl as varchar)
				Select @sql2 = N'Update ' + @BaseHija + N'.dbo.FacStock Set estado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
				Set @sql2 = @sql2 + '; Update ' + @BaseHija + N'.dbo.FacCabeceras Set Idestado=2 where IdCabecera=' + cast(@IdCabeceraRepl as varchar)
				set @sql2 = @sql2 + '; INSERT INTO ' + @BaseHija + N'.dbo.FACCabecerasEstados (IdCabecera, IdEstado, FechaHora, IdUsuario) '
					+N'VALUES (' + cast(@IdCabeceraRepl as varchar) +', 2, getdate(), ' + cast(@IdUsuario as varchar)  +') '

				if @modoDebug > 0 and @modoDebug < 3
					Print @Sql2
				print @sql2
				exec sp_executesql @sql2
			end



		end try
		begin catch
			Print 'Error grave (cabecera - hija 2):'
			SELECT 
				@ErrorMessage =  'Error grave (cabecera - hija 2):' + ERROR_MESSAGE(),
				@ErrorSeverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE();
			RAISERROR (@ErrorMessage, -- Message text.
					   @ErrorSeverity, -- Severity.
					   @ErrorState -- State.
					   );
			if @@trancount > 0
				ROLLBACK tran 	
			return -1
		end catch
	end

	if @modoDebug > 0 and @modoDebug < 3
	Begin
		Print 'Base destino (hija): ' + isnull(@BasePhyDestino, 'NULA')
		Print 'Alta definitiva del comprobante 2 - base hija'
		if @sql is null
			print 'El alta de comprobante produjo una sentencia nula (2)'
	End
	--Delete from @DetallePhy
	--Delete from @tempRemitosBIS
	--Delete from @tempRemitos

	IF OBJECT_ID ('##productosBuscados', 'T') IS Not NULL
		Drop table ##productosBuscados
	-- esta es la llamada final al SP de valorizacion
	IF OBJECT_ID ('syncValorizacion', 'P') IS Not NULL
		exec dbo.syncValorizacion @TWNcRem
	Commit Transaction altaRemitos
	set nocount off
End


--set xact_abort off
--go

