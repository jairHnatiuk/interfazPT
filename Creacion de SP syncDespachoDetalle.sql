USE twinsdb
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


IF not EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'syncDespachoDetalle')
   exec('CREATE Procedure dbo.syncDespachoDetalle  AS BEGIN SET NOCOUNT ON; END')
GO

ALTER procedure [dbo].[syncDespachoDetalle] (@nc_rem int = 0, @interno int = 0)
as
begin
/*
Componente de la interfaz Physis-Twins
Devuelve una grilla detallada de un remito pasado por parametro (nc_rem)
29-8-13
JEH
Recordar que debe actualizarse siempre en todas las DBs!!!!

Agrego un parametro adicional @interno
para 

Van a ver que la formo como UNION, es por la diferencia entre lo que se carga colgado
y lo que no. Cambian dos cosas: 
1) La tropa cuando se trata de cajas no me interesa, la fijo en cero. Si es colgados, si.
2) La cantidad cuando se trata de cajas se calcula de una forma, pero si es colgados no.
Para cajas se cuentan los codigos de barra.
Para colgados se cuentan piezas (puede haber faroles)

Tenemos tres escenarios:

1. Hacienda propia - matrícula de la casa
2. Hacienda de usuario - matrícula de la casa
3. Hacienda de usuario - matrícula de usuario

Hay una tabla en twinsdb que es "usuarios" de la que puedo relacionar nro de usuario - matrícula. 
El nc_u de usuarios se vincula con el usuario de produccion_general.
Eso andaría bárbaro para el primer y último escenario. 

Para el segundo caso, la respuesta de E10 16/7/13:
El tema es así, en la llegada de la tropa, van a asignar una “CALIFICACION”, este será el usuario sin matrícula, 
tenemos pendiente un cambio para que en el palco no se guarde el cambio de la calificación, porque en este caso 
lo usan para imprimir en la etiqueta otro dato, que solo debe estar en la etiqueta, por lo que a futuro a la 
salida del palco, o sea en la producción y faena vas a tener la calificación que también indica el usuario sin 
matrícula, cuando hagan algún cambio de calificación va a representar un cambio de usuario sin matrícula.

Si se implementa el uso de la alternativa 2, en la tabla syncInterfazGralTw debe cargarse
el valor limite (o sea el primer nro de nc_cal que NO debe tratarse como hacienda del usuario).
Por ejemplo, si las pseudo matriculas se identifican con el nc_cal 1 a 7, el valor que se carga es 8
La opcion en ese caso es CalLimite
Si no se implementa esto, no cargar esa opcion en syncInterfazGralTw
*/
	declare @calLimite	int
	declare @ClienteNoEncontrado varchar(20)
	declare @ProductoNoEncontrado varchar(20)
	-- no lo voy a validar aqui porque ya se verifica en el SP syncDespacho
	set @ClienteNoEncontrado = (select valor from syncInterfazGralTw where opcion ='idTerceroNoEncontrado')
	set @calLimite = (select valor from syncInterfazGralTw where opcion ='CalLimite')
	set @ProductoNoEncontrado = (select valor from syncInterfazGralTw where opcion='idProdNoEncontrado')
	If @nc_rem <> 0 and @interno=0
	begin	
	(	
		-- 16/7/15 Agrego soporte para nro de remito no emitido. Si no tiene, usa el nro de carga
		SELECT	
				case when rr.remitonro = '' 
					then right(cast('00000000' as varchar) + cast(rr.nc_rem as varchar),8)
					else rr.remitonro
				end		RemitoTwins, 
				rr.nro_carga			NroCarga, 
				m.codigo				Codigo, 
				m.descripcion			Descripcion, 
				1						Unidades, -- recordar que aca serian cajas
				pg.cantidadmovidap		Peso, 
				0						Usuario, -- no informo usuario
				'0'						Tropa, -- para estos productos no informamos tropa
				rd.nc_rem				CodigoRemitoTwins, 
				case isnull(m.codadm,'0') when '0' then @ProductoNoEncontrado
					when '' then @ProductoNoEncontrado
					else m.codadm end 	CodigoPhysis, 
				pg.mercnro				CodigoInterno, 
				isnull(c.cuit, 0)		cuit, 
				case when ltrim(rtrim(C.cod_adm_cli)) = ''  then @ClienteNoEncontrado else c.cod_adm_cli end CodigoCliente,
				-- anido dos CASE para que el uso del usuario sin matricula sea parametrizable
				case when ISNULL(@calLimite,0) = 0 then '0'
					else case when isnull(pg.nc_cal,0) < @calLimite then '0' else isnull(pg.nc_cal,0) end
				end UsuarioSinMatricula,
				isnull(RP.nPrecio,0)	PrecioUnitario,
				isnull(RP.nPrecio,0)	Precio,
				cast(pg.codbar_s as varchar)			Codbar
		FROM	dbo.[remitos resumen] rr with (nolock)
				inner join 	dbo.[remitos detalle] rd with (nolock) ON rd.nc_rem = rr.nc_rem 
				INNER JOIN dbo.produccion_general pg with (nolock) on PG.codbar_s = rd.codbar and rd.codbar not in ('0','')
				inner join dbo.EXPEDICION_COLECTOR_DETALLE ecd with (nolock) on ecd.codigo = rd.codbar and rd.codbar not in ('0','')
				LEFT JOIN dbo.mercaderia m with (nolock) ON m.nc_mercaderia = rd.mercnro
				left JOIN dbo.clientes c with (nolock) ON c.usuario = rr.usuario
				left join dbo.remitoPrecios RP with(nolock) on RP.ipktwinsrem = Rd.pktwins
		WHERE	(m.grupo not in (select valor from syncInterfazGralTw where opcion='GrupoPartidaSI'))
			and rr.nc_rem = @nc_rem
			and rr.estado <> 'AN'
			and m.codbar='SI'
		)
		UNION
		(	
		SELECT	case when rr.remitonro = '' 
					then right(cast('00000000' as varchar) + cast(rr.nc_rem as varchar),8)
					else rr.remitonro
				end		RemitoTwins, 
				rr.nro_carga			NroCarga, 
				m.codigo				Codigo, 
				m.descripcion			Descripcion, 
				pg.cantidadmovidau		Unidades, 
				pg.cantidadmovidap		Peso, 
				PG.usuario				Usuario,
				case when PATINDEX ( '%(%' , cast(rd.tropa as varchar)	 )=0
					then cast(rd.tropa as varchar)	
					else  0 
				end	
				Tropa, 
				rd.nc_rem				CodigoRemitoTwins, 
				case isnull(m.codadm,'0') when '0' then @ProductoNoEncontrado
					when '' then @ProductoNoEncontrado
					else m.codadm end 	CodigoPhysis, 
				m.nc_mercaderia			CodigoInterno, 
				isnull(c.cuit, 0)		cuit, 
				case when ltrim(rtrim(C.cod_adm_cli)) = ''  then @ClienteNoEncontrado else c.cod_adm_cli end CodigoCliente,
				case when ISNULL(@calLimite,0) = 0 then '0'
					else case when isnull(pg.nc_cal,0) < @calLimite then '0' else isnull(pg.nc_cal,0) end
				end UsuarioSinMatricula,
				isnull(RP.nPrecio,0)	PrecioUnitario,
				isnull(RP.nPrecio,0)	Precio,
				cast(pg.codbar_s as varchar)				Codbar
		FROM	dbo.[remitos resumen] rr with (nolock) 
					inner join (select nc_rem, codbar, mercnro, case when PATINDEX ( '%/%' , cast(tropa as varchar(30))	 )=0
									then cast(tropa as varchar(30))	
									else  left(cast(tropa as varchar(30))	,PATINDEX ( '%/%' , cast(tropa as varchar(30))	 )-1) 
								end	 tropa, pktwins 
								from dbo.[remitos detalle] with (nolock) ) rd ON rd.nc_rem = rr.nc_rem 
					INNER JOIN dbo.produccion_general pg with (nolock) on PG.codbar_s = rd.codbar and rd.codbar not in ('0','')
					LEFT JOIN dbo.mercaderia m with (nolock) ON m.nc_mercaderia = rd.mercnro 
					left JOIN dbo.clientes c with (nolock) ON c.usuario = rr.usuario
					left join dbo.remitoPrecios RP with(nolock) on RP.ipktwinsrem = Rd.pktwins
		WHERE	(m.grupo in (select valor from syncInterfazGralTw where opcion='GrupoPartidaSI')) AND 
					rr.nc_rem = @nc_rem
				and rr.estado <> 'AN'
		)
		UNION -- ultima parte: los codbar NO
		(
			SELECT	
				case when rr.remitonro = '' 
					then right(cast('00000000' as varchar) + cast(rr.nc_rem as varchar),8)
					else rr.remitonro
				end		RemitoTwins, 
				rr.nro_carga			NroCarga, 
				m.codigo				Codigo, 
				m.descripcion			Descripcion, 
				rd.CantidadMovidau		Unidades, -- recordar que aca serian cajas
				rd.cantidadmovidap		Peso, 
				0						Usuario, -- no informo usuario
				case when PATINDEX ( '%/%' , isnull(cast(rd.tropa as varchar)	,'0') )=0
					 and PATINDEX ( '%(%' , isnull(cast(rd.tropa as varchar)	,'0') )=0
					then isnull(cast(rd.tropa as varchar)	,'0')
					else  0  
				end	Tropa, 
				rr.nc_rem				CodigoRemitoTwins, 
				case isnull(m.codadm,'0') when '0' then @ProductoNoEncontrado
					when '' then @ProductoNoEncontrado
					else m.codadm end 	CodigoPhysis, 
				rd.mercnro				CodigoInterno, 
				isnull(c.cuit, 0)		cuit, 
				case when ltrim(rtrim(C.cod_adm_cli)) = ''  then @ClienteNoEncontrado else c.cod_adm_cli end CodigoCliente,
				'0' UsuarioSinMatricula, -- los codbar=NO no llevan este dato
				0	PrecioUnitario,
				0	Precio,
				'0'				Codbar
		FROM	dbo.[remitos resumen] rr with (nolock)
				inner join dbo.[remitos detalle] rd with (nolock) ON rd.nc_rem = rr.nc_rem 
				LEFT JOIN dbo.mercaderia m with (nolock) ON m.nc_mercaderia = rd.mercnro
				left JOIN dbo.clientes c with (nolock) ON c.usuario = rr.usuario
		WHERE	(m.grupo not in (select valor from syncInterfazGralTw where opcion='GrupoPartidaSI'))
			and rr.nc_rem = @nc_rem
			and rr.estado <> 'AN'
			and m.codbar='NO'
		)
	end
	If @nc_rem <> 0 and @interno=1
	begin	
		-- 18/4/18 Soporte para remitos internos
		-- OJO! es requisito que el USUARIO de faena esté cargado como CLIENTE
		-- y si se repite el CUIT de ese cliente, que el cliente que corresponde al usuario
		-- sea el primero de todos (el minimo del campo usuario de la tabla clientes)
		(
		SELECT	
				case when rr.sNroRemito = '' 
					then right(cast('00000000' as varchar) + cast(rr.iNcRem as varchar),8)
					else rr.sNroRemito
				end		RemitoTwins, 
				0						NroCarga, 
				m.codigo				Codigo, 
				m.descripcion			Descripcion, 
				pg.cantidadmovidau		Unidades, 
				pg.cantidadmovidap		Peso, 
				PG.usuario				Usuario,
				case when PATINDEX ( '%(%' , cast(pg.tropa as varchar)	 )=0
					then cast(pg.tropa as varchar)	
					else  0 
				end	
				Tropa, 
				rr.iNcRem				CodigoRemitoTwins, 
				case isnull(m.codadm,'0') when '0' then @ProductoNoEncontrado
					when '' then @ProductoNoEncontrado
					else m.codadm end 	CodigoPhysis, 
				m.nc_mercaderia			CodigoInterno, 
				isnull(cl.cuit, 0)		cuit, 
				case when ltrim(rtrim(CL.cod_adm_cli)) = ''  then @ClienteNoEncontrado else cl.cod_adm_cli end CodigoCliente,
				0	UsuarioSinMatricula,
				0	PrecioUnitario,
				0	Precio,
				cast(pg.codbar_s as varchar)				Codbar
		FROM	dbo.RemitosInternosResumen rr with (nolock) 
					inner join dbo.RemitosInternosDetalle rd ON rd.iNcRem = rr.iNcRem 
					INNER JOIN dbo.produccion_general pg with (nolock) on PG.PktwinsPG = rd.iPkTwinsPG
					inner join dbo.USUARIOS U on U.NC_U = rr.iDestino
					inner JOIN dbo.mercaderia m with (nolock) ON m.nc_mercaderia = pg.mercnro 
					left join dbo.CLIENTES CL on U.NC_U = CL.nc_U
					--left JOIN (select MIN(usuario) usuario, cuit from dbo.clientes where Activo='SI'
					--group by cuit ) C  on C.CUIT=U.cuit
					--left join dbo.clientes CL on CL.usuario=C.usuario
					
		WHERE	(m.grupo in (select valor from syncInterfazGralTw where opcion='GrupoPartidaSI')) AND 
					rr.iNcRem = @nc_rem
				and rr.sestado <> 'AN'
		)
		UNION
		(
				SELECT
				case when rr.sNroRemito = '' 
					then right(cast('00000000' as varchar) + cast(rr.iNcRem as varchar),8)
					else rr.sNroRemito
				end		RemitoTwins, 
				0						NroCarga, 
				m.codigo				Codigo, 
				m.descripcion			Descripcion, 
				1						Unidades, -- recordar que aca serian cajas
				pg.cantidadmovidap		Peso, 
				0						Usuario, -- no informo usuario
				'0'						Tropa, -- para estos productos no informamos tropa

				rr.iNcRem				CodigoRemitoTwins, 
				case isnull(m.codadm,'0') when '0' then @ProductoNoEncontrado
					when '' then @ProductoNoEncontrado
					else m.codadm end 	CodigoPhysis, 
				m.nc_mercaderia			CodigoInterno, 
				isnull(cl.cuit, 0)		cuit, 
				case when ltrim(rtrim(CL.cod_adm_cli)) = ''  then @ClienteNoEncontrado else cl.cod_adm_cli end CodigoCliente,
				0	UsuarioSinMatricula,
				0	PrecioUnitario,
				0	Precio,
				cast(pg.codbar_s as varchar)				Codbar
		FROM	dbo.RemitosInternosResumen rr with (nolock) 
					inner join dbo.RemitosInternosDetalle rd ON rd.iNcRem = rr.iNcRem 
					INNER JOIN dbo.produccion_general pg with (nolock) on PG.PktwinsPG = rd.iPkTwinsPG
					inner join dbo.USUARIOS U on U.NC_U = rr.iDestino
					inner JOIN dbo.mercaderia m with (nolock) ON m.nc_mercaderia = pg.mercnro 
					left join dbo.CLIENTES CL on U.NC_U = CL.nc_U
					--left JOIN (select MIN(usuario) usuario, cuit from dbo.clientes where Activo='SI'
					--group by cuit ) C  on C.CUIT=U.cuit
					--left join dbo.clientes CL on CL.usuario=C.usuario
		WHERE	(m.grupo not in (select valor from syncInterfazGralTw where opcion='GrupoPartidaSI'))
			and rr.iNcRem = @nc_rem
			and rr.sEstado <> 'AN'
			and m.codbar='SI'
		)
	end
end


