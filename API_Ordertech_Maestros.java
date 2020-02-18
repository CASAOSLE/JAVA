import faf.app.base.util.api.APIFinnegUtils;
import faf.app.base.common.CommonFunctionsHLP;
import faf.app.base.data.dataset.DatasetUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import faf.app.base.util.JsonUtils;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import faf.app.base.security.session.SessionHLP;
import faf.app.base.security.session.model.SessionVO;

String sessionID = params.getSessionID();
DBHelper dbh = params.getDBHelper();
CasoBPMAccionVO accion = params.get("Accion");
UtilsHLP utils = new UtilsHLP(sessionID);
SessionVO session = SessionHLP.getSession(sessionID);
   
String tokenkey = "API-TOKEN";
String tokenvalue = "osle-6R3DURRQ5F48J3V4MITVJGIVUDDXE31MBWDR5TYU";

//Rubros 
String url = "http://customers.ordertech.com.ar/ordertech/current/distrot/rubros";
URL api = new URL(url);

Vector tipos = new Vector();
tipos.add(String.class);
tipos.add(String.class);
Vector valores = new Vector();
valores.add("Maestros");
valores.add(accion.getDescripcion());
DatasetUtil.runSPForUpdate(dbh, session, "USR_OT_Logger", tipos, valores, false);
String log = "";

try{
    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    log += dateFormat.format(new Date());
    
	String query = "SELECT Nombre, Codigo = CAST(Codigo AS INT), Activo FROM USR_COD_RUB WHERE USR_OT = 1";
	String json = "{\"rubros\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"id\":"+rs.getString("Codigo")+", \"descripcion\":\""+rs.getString("Nombre")+"\", \"activo\":"+rs.getString("Activo")+", \"prioridad\":10 },";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";
	
	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
    JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
	log += "\n"+ex.getMessage();
}

//Familias
url = "http://customers.ordertech.com.ar/ordertech/current/distrot/familias";
api = new URL(url);
try{
	String query = "SELECT F.Nombre, Codigo = CAST(F.Codigo AS INT), Rubro = CAST(R.Codigo AS INT), Prioridad = ISNULL(F.Orden, 10) "+
		"FROM USR_COD_FAM F INNER JOIN USR_COD_RUB R ON F.Rubro = R.RubroID WHERE USR_OT = 1";
	String json = "{\"familias\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"id\":"+rs.getString("Codigo")+", \"descripcion\":\""+rs.getString("Nombre")+
		"\", \"id_rubro\":"+rs.getString("Rubro")+", \"prioridad\":"+rs.getString("Prioridad")+"},";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";
	
	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
    JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
    log += "\n"+ex.getMessage();
}

//Productos
url = "http://customers.ordertech.com.ar/ordertech/current/distrot/articulos";
api = new URL(url);
try{
	String query = "SELECT Codigo = RIGHT(P.Codigo, 8), P.Nombre, Rubro = CAST(R.Codigo AS INT), "+
		"Linea = ISNULL(USR_Linea, 1), PackPor = ISNULL(USR_PackPor, 1), Fraccion = ISNULL(USR_Fraccion, 1), "+
		"Prioridad = ISNULL(USR_Prioridad, 1), P.Activo, Familia = CAST(F.Codigo AS INT) "+
		"FROM BSProducto P "+ //Solo los rubros marcados, sacando excluídos
		"INNER JOIN USR_COD_RUB R ON P.USR_COD_RUB = R.RubroID "+
		"INNER JOIN USR_COD_FAM F ON R.RubroID = F.Rubro AND P.USR_COD_FAM = F.FamiliaID "+
		"WHERE R.USR_OT = 1 AND ISNULL(P.USR_OT_Excluir, 0) = 0"; 
	String json = "{\"articulos\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"codigo\":\""+rs.getString("Codigo")+
		"\",\"descripcion\":\""+rs.getString("Nombre")+
		"\",\"id_rubro\":"+rs.getString("Rubro")+
		",\"linea\":"+rs.getString("Linea")+
		",\"packpor\":"+rs.getString("PackPor")+
		",\"fraccion\":"+rs.getString("Fraccion")+
		",\"prioridad\":"+rs.getString("Prioridad")+
		",\"activo\":"+rs.getString("Activo")+
		",\"id_familia\":"+rs.getString("Familia")+
		",\"accion\":\"<>\"},";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";
	
	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
	JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
   log += "\n"+ex.getMessage();
}

//Precios 
url = "http://customers.ordertech.com.ar/ordertech/current/distrot/precios";
api = new URL(url);
try{
	String query = "SELECT Producto = RIGHT(P.Codigo, 8), Lista = LP.Codigo, Precio = LPI.Precio-ISNULL(P.ImporteImpuestosInternos, 0), "+
		"II = ISNULL(P.ImporteImpuestosInternos, 0), Sugerido = ISNULL(USR_Precio_Vta_Sugerido, 0) "+
		"FROM BSProducto P "+ //Solo los rubros marcados, por ahora sólo de massalin
		"INNER JOIN USR_COD_RUB R ON P.USR_COD_RUB = R.RubroID "+
		"INNER JOIN BSListaPrecio LP ON LP.Codigo = 'Escala10' "+
		"INNER JOIN BSListaPrecioItem LPI ON LP.ListaPrecioID = LPI.ListaPrecioID AND LPI.ProductoID = P.ProductoID "+
		"WHERE R.USR_OT = 1 AND ISNULL(P.USR_OT_Excluir, 0) = 0 AND ISNULL(LPI.Precio, 0) <> 0";
	String json = "{\"precios\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"codigo_articulo\":\""+rs.getString("Producto")+
		"\",\"lista\":\""+rs.getString("Lista")+
		"\",\"precio\":"+rs.getString("Precio")+
		",\"impuestos_internos\":"+rs.getString("II")+
		",\"pvp\":"+rs.getString("Sugerido")+
		",\"segmento\":1},";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";
	
	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
    JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
   log += "\n"+ex.getMessage();
}

//Sucursales 
url = "http://customers.ordertech.com.ar/ordertech/current/distrot/sucursales";
api = new URL(url);
try{
	String query = "SELECT Codigo, Nombre, Direccion = dbo.getDireccion(31, EmpresaID, NULL, 2), Horarios = ISNULL(USR_Horarios, '') "+
		"FROM FAFEmpresa WHERE USR_OT = 1"; 
	String json = "{\"sucursales\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"codigo\":\""+rs.getString("Codigo")+
		"\",\"nombre\":\""+rs.getString("Nombre")+
		"\",\"direccion\":\""+rs.getString("Direccion")+
		"\",\"horarios\":\""+rs.getString("Horarios")+"\"},";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";
	
	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
    JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
   log += "\n"+ex.getMessage();
}

//Clientes 
url = "http://customers.ordertech.com.ar/ordertech/current/distrot/clientes";
api = new URL(url);
try{
	String query = "SELECT O.Codigo, O.Nombre, O.CUIT, SucursalOsle = E.Codigo, "+
		"CategoriaFiscal = CASE CF.Codigo WHEN 'NO_CATEGORIZADO' THEN 'NOCAT' WHEN 'FCEGRANRI' THEN 'RI' ELSE CF.Codigo END, "+
		"Zona = ISNULL(USR_Zona, ''), Frecuencia = ISNULL(USR_Frecuencia, ''), Pais = ISNULL(P.Nombre, ''), "+
		"Provincia = ISNULL(Pr.Nombre, ''), Localidad = ISNULL(L.Nombre, ''), "+
		"Telefono = ISNULL((SELECT TOP 1 Numero FROM BSTelefono WHERE OrganizacionID = O.OrganizacionID), ''), "+
		"Direccion = ISNULL(D.Calle, '')+' '+ISNULL(D.Numero, '')+' Dep.: '+ISNULL(D.Dpto, '') "+
		"FROM BSOrganizacion O LEFT JOIN FAFEmpresa E ON O.USR_SucursalOsle = E.EmpresaID "+
		"LEFT JOIN BSCategoriaFiscal CF ON O.CategoriaFiscalID = CF.CategoriaFiscalID "+
		"LEFT JOIN BSDireccion D ON O.OrganizacionID = D.OrganizacionID AND D.EsPrincipal = 1 "+
		"LEFT JOIN BSPais P ON D.PaisID = P.PaisID LEFT JOIN BSProvincia Pr ON D.ProvinciaID = Pr.ProvinciaID "+
		"LEFT JOIN BSLocalidad L ON D.LocalidadID = L.LocalidadID WHERE O.USR_OT = 1"; 
	String json = "{\"clientes\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"codigo\":"+rs.getString("Codigo")+
		",\"nombre\":\""+rs.getString("Nombre")+
		"\",\"direccion\":\""+rs.getString("Direccion")+
		"\",\"latitud\":\""+
		"\",\"longitud\":\""+
		"\",\"pais\":\""+rs.getString("Pais")+
		"\",\"provincia\":\""+rs.getString("Provincia")+
		"\",\"localidad\":\""+rs.getString("Localidad")+
		"\",\"telefono\":\""+rs.getString("Telefono")+
		"\",\"iibb_per\":0"+
		",\"descuento\":0"+
		",\"zona\":\""+rs.getString("Zona")+
		"\",\"lista_precios\":\"Escala10"+ //rs.getString("Escala")+ Por ahora fijo
		"\",\"cuit\":\""+rs.getString("Cuit")+
		"\",\"frecuencia\":\""+rs.getString("Frecuencia")+
		"\",\"iva_porcentaje\":0.21"+ //OT maneja un sólo porcentaje de IVA por cliente
		",\"tipo_iva\":\""+rs.getString("CategoriaFiscal")+
		"\",\"sucursal_codigo\":\"SUC20"+ //rs.getString("SucursalOsle")+ Por ahora fijo
		"\",\"accion\":\"+"+
		"\"},";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";

	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
    JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
   log += "\n"+ex.getMessage();
}

//Usuarios 
url = "http://customers.ordertech.com.ar/ordertech/current/distrot/usuarios";
api = new URL(url);
try{
	String query = "SELECT O.Codigo, Nombre = USR_OTU_Nombre, Pass = USR_OTU_Pass "+
		"FROM BSOrganizacion O WHERE USR_OT = 1 AND ISNULL(USR_OTU_Nombre, '') <> '' AND ISNULL(USR_OTU_Pass, '') <> ''"; 
	String json = "{\"usuarios\": [";
	ResultSet rs = dbh.executeResultSet(query);
	while(rs.next()) json += "{\"cliente\":"+rs.getString("Codigo")+
		",\"nombre\":\""+rs.getString("Nombre")+
		"\",\"email\":\""+rs.getString("Nombre")+
		"\",\"password\":\""+rs.getString("Pass")+"\"},";
	rs.close();
	json = CommonFunctionsHLP.removeLastComma(json);
	json += "]}";
	
	HttpURLConnection urlConnection = api.openConnection();
	urlConnection.setRequestMethod("PUT");
	urlConnection.setRequestProperty("Content-Type", "application/json");
	urlConnection.setRequestProperty(tokenkey, tokenvalue);
	urlConnection.setDoOutput(true);
	urlConnection.setDoInput(true);
	urlConnection.connect();
	
	OutputStream os = urlConnection.getOutputStream();
	OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
	osw.write(json);
	osw.flush();
	StringBuffer respuesta = APIFinnegUtils.getResponse(urlConnection);
	
	osw.close();
	os.close();
	urlConnection.disconnect();
	
	JsonParser parser = new JsonParser();
    JsonElement jsonResponse = parser.parse(respuesta.toString());
	JsonObject jsonObjectResponse = jsonResponse.getAsJsonObject();
	log += "\n"+jsonObjectResponse.get("response").toString();
}
catch(Exception ex){
   log += "\n"+ex.getMessage();
}
accion.setDescripcion(log);