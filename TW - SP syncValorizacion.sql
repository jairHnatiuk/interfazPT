USE [TwinsDb]
GO
/****** Object:  StoredProcedure [dbo].[syncValorizacion]    Script Date: 10/04/2015 07:33:40 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



IF not EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'syncValorizacion')
   exec('CREATE Procedure dbo.syncValorizacion  AS BEGIN SET NOCOUNT ON; END')
GO


ALTER procedure [dbo].[syncValorizacion] (@ncRem int=0, @modoDebug int=0) as
begin
	/*
		Asignacion de precios a remitos
		En Runfo al realizarse la expedicion se asignan los precios a las medias reses, segun los
		valores de la tabla Calificaciones3
		En la tabla RemitoPrecios se almacenan los precios.
		Este SP cumple la funcion de generar los registros de RemitoPrecios con los precios
		que corresponden por calificacion 3 al momento de ejecutarse.
		06/04/2015 . Jair Hnatiuk
		Tecnicamente esto corresponde a Twins, pero a pedido de ECatino lo estoy resolviendo por mi cuenta
	*/
	set nocount on
	-- solo lo habilito para el punto de venta 0040, el de vacaitor
	-- de esta forma evito pegar registros en RemitoPrecios de los puntos de venta que no corresponden
	-- OOOOOJJOOOOOOOO que en otras implementaciones usan el campo EMPRESA en vez de sucursal
	if @ncRem>0
	begin
		if exists (select 1 from [REMITOS RESUMEN] where nc_rem=@ncRem and sucursal='0040' or sucursal='0004')
			insert RemitoPrecios(iNcRem, iNcMercaderia, nPrecio,ifechaSys,ifechaMod,sHorasys,sHoramod,incpc,srealizo,ipktwinsRem)
				select RD.nc_rem
					,Rd.mercnro
					,c3.nPrecio
					,CONVERT(VARCHAR(10), GETDATE(), 112) fechasys
					,CONVERT(VARCHAR(10), GETDATE(), 112) fechaMod
					,CONVERT(varchar,GETDATE(),108) horasys
					,CONVERT(varchar,GETDATE(),108) horamod
					,1 iNcPC
					,'Interfaz' sRealizo
					,RD.pktwins
				from  [REMITOS RESUMEN] RR inner join 
					[REMITOS DETALLE] RD on rr.nc_rem = rd.nc_rem
					inner join faena F on rd.codbar=f.codbar
					inner join Calificaciones3 c3 on f.Nc_Cal3 = c3.iNcCal3
				where rd.nc_rem = @ncrem
					and rr.estado <> 'AN'
		if @modoDebug > 0
			Print 'SVA. Se valoriza el remito con nc ' + cast(@ncrem as varchar)
	end				
	else
		print 'Debe indicarse por parametro un NC_REM valido para que se valorice'
	-- el campo iPkTwins de RemitoPrecios es autoincrementado
end



