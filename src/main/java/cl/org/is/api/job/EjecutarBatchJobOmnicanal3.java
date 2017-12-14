/**
 *@name CopiarArchivosCuadraturaJob2.java
 * 
 *@version 1.0 
 * 
 *@date 30-03-2017
 * 
 *@author EA7129
 * 
 *@copyright Cencosud. All rights reserved.
 */
package cl.org.is.api.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
//import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCDriver;

/**
 * @description 
 */
public class EjecutarBatchJobOmnicanal3 {
	
	private static BufferedWriter bw;
	private static String path;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			//path                   =  "C:\\PROYECTOS\\C8INVENTARIOS\\MASIVOS\\";
			//info                   = new File(path+"/LOG/info.txt");
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
	}
	
	private static void crearTxt(Map<String, String> mapArguments) {
		// TODO Auto-generated method stub
		Connection dbconnection = crearConexion();
		Connection dbconnOracle = crearConexionOracle();
		File fileComparativo              = null;
		File fileWms              = null;
		File fileJda              = null;
		BufferedWriter bwComparativo       = null;
		BufferedWriter bwWms      = null;
		BufferedWriter bwJda      = null;
		PreparedStatement pstmtComparativo = null;
		PreparedStatement pstmtWms = null;
		PreparedStatement pstmtJda = null;
		StringBuffer sbComparativo         = null;
		StringBuffer sbWms         = null;
		StringBuffer sbJda         = null;
		byte[] buffer = new byte[1024];
		//int iFechaIni           = 0;
		//int iFechaFin           = 0;
		Date now2 = new Date();
		SimpleDateFormat ft2 = new SimpleDateFormat ("dd/MM/YY");
		String currentDate2 = ft2.format(now2);
		
		Date now3 = new Date();
		SimpleDateFormat ft3 = new SimpleDateFormat ("YYYYMMdd");
		String currentDate3 = ft3.format(now3);
		
		
		
		

		try {

			try {
				//iFechaIni = restarDia(mapArguments.get("-fi"));
				//iFechaFin = restarDia(mapArguments.get("-ff"));
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
			//Runtime.getRuntime().exec("C:\\PROYECTOS\\C8INVENTARIOS\\PROC_INVD.bat");
			
			//Date now = new Date();
			//SimpleDateFormat ft = new SimpleDateFormat ("dd-MM-YYYY");
			//String currentDate = ft.format(now);
			
			//String iFechaIni = currentDate;
			//String iFechaFin = currentDate;
			
			
			//fileComparativo           = new File(path + currentDate + "/COMPARATIVO_ASN_DIARIO-" + currentDate + ".txt");
			fileComparativo           = new File(path + "/InventarioComparativoDiario-" + currentDate3 + ".txt");
			fileWms                   = new File(path + "/InventarioWmsDiario-" + currentDate3 + ".txt");
			fileJda                   = new File(path + "/InventarioJdaDiario-" + currentDate3 + ".txt");
			
			sbComparativo = new StringBuffer();
			sbWms = new StringBuffer();
			sbJda = new StringBuffer();
			
			
			
			
			/*###########################ARCHIVO COMPARATIVO###########################*/
			sbComparativo.append("Select Pix_Tran.REF_FIELD_1, to_char(MAX(MOD_DATE_TIME),'DD-MM-YYYY') AS MOD_DATE_TIME, SUM(Pix_Tran.UNITS_RCVD) AS TOTAL FROM  Pix_Tran Pix_Tran where 1 = 1 AND Pix_Tran.Mod_Date_Time >= (TO_DATE('"+currentDate2+"','DD/MM/YY')-1) AND Pix_Tran.Mod_Date_Time < to_date('"+currentDate2+"','DD/MM/YY') AND Pix_Tran.Tran_Type='603' AND Pix_Tran.Tran_Code='02'  AND Pix_Tran.Whse='012' GROUP BY Pix_Tran.REF_FIELD_1");
			System.out.println("Sql Comparativo: " + sbComparativo);
			pstmtComparativo         = dbconnOracle.prepareStatement(sbComparativo.toString());
			//pstmt.setInt(1, iFechaIni);
			//pstmt.setInt(2, iFechaFin);
			sbComparativo = new StringBuffer();
			ResultSet rsComparativo = pstmtComparativo.executeQuery();
			
			bwComparativo  = new BufferedWriter(new FileWriter(fileComparativo));
			bwComparativo.write("CALCULADO;");
			bwComparativo.write("REF_FIELD_1;");
			bwComparativo.write("TOTAL;");
			bwComparativo.write("ASN;");
			bwComparativo.write("CANT;");
			bwComparativo.write("DIF;");
			bwComparativo.write("ESTADO;\n");
			
			while (rsComparativo.next()) {
				bwComparativo.write(rsComparativo.getString("MOD_DATE_TIME") + ";");
				bwComparativo.write(rsComparativo.getString("REF_FIELD_1") + ";");
				bwComparativo.write(rsComparativo.getString("TOTAL") + ";");
				
				if (rsComparativo.getString("REF_FIELD_1") != null) {

					bwComparativo.write(ejecutarQuery2(rsComparativo.getString("REF_FIELD_1"), rsComparativo.getString("TOTAL"), dbconnection, null));
				}
				
			}
			
			//Thread.sleep(60000);
			Thread.sleep(60);
			System.out.println("Regreso del WMS sleep(60 seg): " );
			
			/*###########################ARCHIVO WMS DIARIO###########################*/
			
			sbWms.append("select  Pix_Tran.REF_FIELD_1,Pix_Tran.TRAN_NBR,to_char(MOD_DATE_TIME,'DD-MM-YYYY') AS Calculado,MOD_DATE_TIME,Pix_Tran.UNITS_RCVD,Pix_Tran.WHSE, Pix_Tran.Tran_Type, Pix_Tran.Tran_Code FROM Pix_Tran  where 1 = 1 AND Pix_Tran.Mod_Date_Time >= (TO_DATE('"+currentDate2+"','DD/MM/YY')-1) AND Pix_Tran.Mod_Date_Time < to_date('"+currentDate2+"','DD/MM/YY') AND Pix_Tran.Tran_Type='603'  AND Pix_Tran.Tran_Code='02'  AND Pix_Tran.Whse='012'");
			System.out.println("Sql sbWms: " + sbWms);
			pstmtWms         = dbconnOracle.prepareStatement(sbWms.toString());
			//pstmt.setInt(1, iFechaIni);
			//pstmt.setInt(2, iFechaFin);
			sbWms = new StringBuffer();
			ResultSet rsWms = pstmtWms.executeQuery();
			
			bwWms  = new BufferedWriter(new FileWriter(fileWms));
			bwWms.write("REF_FIELD_1;");
			bwWms.write("TRAN_NBR;");
			bwWms.write("CALCULADO;");
			bwWms.write("UNITS_RCVD;");
			bwWms.write("MOD_DATE_TIME;");
			bwWms.write("WHSE;\n");
			
			while (rsWms.next()) {
				bwWms.write(rsWms.getString("REF_FIELD_1") + ";");
				bwWms.write(rsWms.getString("TRAN_NBR") + ";");
				bwWms.write(rsWms.getString("CALCULADO") + ";");
				bwWms.write(rsWms.getString("UNITS_RCVD") + ";");
				bwWms.write(rsWms.getString("MOD_DATE_TIME") + ";");
				bwWms.write(rsWms.getString("WHSE") + "\n");

			}
			
			//Thread.sleep(60000);
			Thread.sleep(60);
			System.out.println("Regreso del WMS sleep(60 seg): " );
			
			/*###########################ARCHIVO JDA###########################*/
			//sbJda.append("Select P.Pordat, E.Citaan, P.Pounts, P.Poloc, P.Ponumb, P.Ponrcv  FROM  exisbugd.Exiff17G E, mmsp4lib.Pomrch P    WHERE (P.Ponasn=E.Citaac) AND (P.Poloc=12  AND P.Pordat>160619 AND E.Citaan=609449)");
			sbJda.append("Select P.Pordat, E.Citaan, P.Pounts, P.Poloc, P.Ponumb, P.Ponrcv  FROM  exisbugd.Exiff17G E, mmsp4lib.Pomrch P    WHERE (P.Ponasn=E.Citaac) AND (P.Poloc=12  AND P.Pordat>160619)");
			//System.out.println("Sql sbJda: " + sbJda);
			pstmtJda         = dbconnection.prepareStatement(sbJda.toString());
			//pstmt.setInt(1, iFechaIni);
			//pstmt.setInt(2, iFechaFin);
			sbJda = new StringBuffer();
			ResultSet rsJda = pstmtJda.executeQuery();
			
			bwJda  = new BufferedWriter(new FileWriter(fileJda));
			bwJda.write("Pordat;");
			bwJda.write("Citaan;");
			bwJda.write("Pounts;");
			bwJda.write("Poloc;");
			bwJda.write("Ponumb;\n");
			//bwJda.write("Ponrcv;\n");
			
			while (rsJda.next()) {
				bwJda.write(rsJda.getString("Pordat") + ";");
				bwJda.write(rsJda.getString("Citaan") + ";");
				bwJda.write(rsJda.getString("Pounts") + ";");
				bwJda.write(rsJda.getString("Poloc") + ";");
				bwJda.write(rsJda.getString("Ponumb") + "\n");
				//bwJda.write(rsJda.getString("Ponrcv") + "\n");
			}
			
			Thread.sleep(60);
			System.out.println("Sleep de (60 seg) para comprimir archivo comparativo: " );
			info("Archivos creados.");
		}
		catch (Exception e) {
			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(dbconnOracle,pstmtComparativo,bwComparativo);
			cerrarTodo(dbconnOracle, pstmtWms, bwWms);
			cerrarTodo(dbconnection, pstmtJda, bwJda);
		}
		
		
		try{

			FileOutputStream fos = new FileOutputStream(path + "/InventarioComparativoDiario-" + currentDate3 + ".zip");
    		ZipOutputStream zos = new ZipOutputStream(fos);
    		ZipEntry ze= new ZipEntry(path + "/InventarioComparativoDiario-" + currentDate3 + ".txt");
    		zos.putNextEntry(ze);
    		FileInputStream in = new FileInputStream(path + "/InventarioComparativoDiario-" + currentDate3 + ".txt");
			
    		int len;
    		while ((len = in.read(buffer)) > 0) {
    			zos.write(buffer, 0, len);
    		}
    		in.close();
    		zos.closeEntry();
    		//remember close it
    		zos.close();
    		
    		//Thread.sleep(60);
			//System.out.println("Sleep de (60 seg) para comprimir archivo Wms: " );
    		
    		FileOutputStream fosWms = new FileOutputStream(path + "/InventarioWmsDiario-" + currentDate3 + ".zip");
    		ZipOutputStream zosWms = new ZipOutputStream(fosWms);
    		ZipEntry zeWms = new ZipEntry("InventarioWmsDiario-" + currentDate3 + ".txt");
    		zosWms.putNextEntry(zeWms);
    		FileInputStream inWms = new FileInputStream(path + "/InventarioWmsDiario-" + currentDate3 + ".txt");
			
    		int lenWms;
    		while ((lenWms = inWms.read(buffer)) > 0) {
    			zosWms.write(buffer, 0, lenWms);
    		}
    		inWms.close();
    		zosWms.closeEntry();
    		//remember close it
    		zosWms.close();
    		
    		//Thread.sleep(60);
			//System.out.println("Sleep de (60 seg) para comprimir archivo Jda: " );
			
    		FileOutputStream fosJda = new FileOutputStream(path + "/InventarioJdaDiario-" + currentDate3 + ".zip");
    		ZipOutputStream zosJda = new ZipOutputStream(fosJda);
    		ZipEntry zeJda = new ZipEntry("InventarioJdaDiario-" + currentDate3 + ".txt");
    		zosJda.putNextEntry(zeJda);
    		FileInputStream inJda = new FileInputStream(path + "/InventarioJdaDiario-" + currentDate3 + ".txt");
			
    		int lenJda;
    		while ((lenJda = inJda.read(buffer)) > 0) {
    			zosJda.write(buffer, 0, lenJda);
    		}
    		inJda.close();
    		zosJda.closeEntry();
    		//remember close it
    		zosJda.close();
    		//Thread.sleep(60);
			//System.out.println("Sleep de (60 seg) para eliminar archivos: " );

    		System.out.println("Done");

    	} catch(IOException ex){
    	   ex.printStackTrace();
    	}
		
		//fileComparativo           = new File(path + "/InventarioComparativoDiario-" + currentDate3 + ".txt");
		//fileWms                   = new File(path + "/InventarioWmsDiario-" + currentDate3 + ".txt");
		//fileJda                   = new File(path + "/InventarioJdaDiario-" + currentDate3 + ".txt");
		
		//eliminarArchivo(path + "/InventarioComparativoDiario-" + currentDate3 + ".txt");
		//eliminarArchivo(path + "/InventarioWmsDiario-" + currentDate3 + ".txt");
		//eliminarArchivo(path + "/InventarioJdaDiario-" + currentDate3 + ".txt");
	}
	
	private static String ejecutarQuery2(String Citaan, String total, Connection dbconnection, PreparedStatement pstmtCuadratura) {

		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;

		try {

			sb = new StringBuffer();
			//sb.append("Select CASE WHEN E.Citaan IS NOT NULL THEN E.Citaan ELSE '0' END  AS ASN, SUM(P.Pounts) AS Cantidad  FROM  exisbugd.Exiff17G E, mmsp4lib.Pomrch P    WHERE (P.Ponasn=E.Citaac) AND (P.Poloc=12  AND P.Pordat>160619 AND E.Citaan='"+Citaan+"') GROUP BY E.Citaan");
			sb.append("Select E.Citaan  AS ASN, SUM(P.Pounts) AS Cantidad  FROM  exisbugd.Exiff17G E, mmsp4lib.Pomrch P    WHERE (P.Ponasn=E.Citaac) AND (P.Poloc=12  AND P.Pordat>160619 AND E.Citaan='"+Citaan.replace("T", "").replace("C", "")+"') GROUP BY E.Citaan");
			//System.out.println("Sql sb: " + sb);
			//sb.append("SELECT Invaud.Itrloc, Invaud.INUMBR, Invaud.ITRTYP, Invaud.ITRDAT, Invaud.ITPDTE, Invaud.IDEPT, Invaud.ITRREF, Invaud.ITRAF1 FROM mmsp4lib.Invaud Invaud WHERE (Invaud.Itrtyp=31 AND Invaud.Itrdat>170101 AND Invaud.Itrloc=12)");
			pstmt = dbconnection.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			sb = new StringBuffer();

			boolean reg = false;
			do{
				reg = rs.next();
				if (reg){
					sb.append( String.valueOf((Integer.parseInt(rs.getString("Cantidad")) > 0 ? rs.getString("ASN") : "#N/A") ) + ";");
					//sb.append(rs.getString("Cantidad") + ";");
					sb.append( String.valueOf((Integer.parseInt(rs.getString("Cantidad")) > 0 ? rs.getString("Cantidad") : "#N/A") ) + ";");
					//sb.append(( (Integer.parseInt(rs.getString("Cantidad")) - Integer.parseInt(total)))  + ";");
					sb.append( String.valueOf((Integer.parseInt(rs.getString("Cantidad")) > 0 ? ( (Integer.parseInt(rs.getString("Cantidad")) - Integer.parseInt(total))) : "#N/A") ) + ";");
					sb.append(obtenerEstado(rs.getString("ASN") , rs.getString("Cantidad"), (Integer.parseInt(rs.getString("Cantidad")) - Integer.parseInt(total))) + "\n");
					

					break;
				}else{
					//sb.append("\n");
					
					sb.append("#N/A" + ";");
					sb.append("#N/A" + ";");
					sb.append("#N/A" + ";");
					sb.append("#N/A" + "\n");

				}
			}
			while (reg);
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearTxt2]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null,pstmt,null);
		}
		return sb.toString();
	}
	
	
	public static void eliminarArchivo(String archivo){

	     File fichero = new File(archivo);
	   
	     if(fichero.delete()){

	          System.out.println("archivo eliminado");
	    
	     } else {
	    	 System.out.println("archivo no eliminado");
	     }

	}  
	

	private static Connection crearConexion() {

		System.out.println("Creado conexion a ROBLE.");
		AS400JDBCDriver d = new AS400JDBCDriver();
		String mySchema = "RDBPARIS2";
		Properties p = new Properties();
		AS400 o = new AS400("roble.cencosud.corp","USRCOM", "USRCOM");
		Connection dbconnection = null;

		try {
			info("AuthenticationScheme: "+o.getVersion());
			dbconnection = d.connect (o, p, mySchema);
			info("Conexion a ROBLE CREADA.");
		}
		catch (Exception e) {
			info("Error de conexion roble:" + e.getMessage());
		}
		return dbconnection;
	}

	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

			//Comentado por cambio de base de datos. El servidor g500603svcr9 corresponde shareplex.
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9:1521:MEOMCLP","REPORTER","RptCyber2015");
			
			//El servidor g500603sv0zt corresponde a Producción.
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svbbr:1521:REPORTMHN","CONWMS","CONWMS");
		}
		catch (Exception e) {
			info("Error de conexion Wms:" + e.getMessage());
		}
		return dbconnection;
	}
	
	
	/*
	private static String limpiarCeros(String str) {

		int iCont = 0;

		while (str.charAt(iCont) == '0') {

			iCont++;
		}
		return str.substring(iCont, str.length());
	}
	*/

	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {
			info("Error de creacion archivo:" + e.getMessage());
		}
	}
	/*
	private static int restarDia(String sDia) {

		int dia = 0;
		String sFormato = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormato);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -1);
			sDiaAux = df.format(diaAux.getTime());
			dia = Integer.parseInt(sDiaAux);
		}
		catch (Exception e) {

			info("[restarDia]Exception:"+e.getMessage());
		}
		return dia;
	}
	*/
	
	
	/**
	 * Metodo que calcula la formula de cumplimiento de ordenes de compra
	 * 
	 * @param String, cumplimiento calidad compra
	 * @param String, cumplimiento beetrack compra
	 * @param String, cumplimiento Eom de compra
	 * @param String, cruce del sub Estado de compra
	 * @param String, cruce estado de compra
	 * @param String, estado de linea de compra
	 * @param String, estado calce beetrak de compra
	 * @param String, numero de guia de compra
	 * @return String indicando el estado de orden de compra
	 * 
	 */
	private static String obtenerEstado(String asn, String cantidad, int dif) {
		// TODO Auto-generated method stub
		String result = null;
		if ("0".equals(asn) || asn == null) {
			result = "Sin Actualizar";
		} else if (Integer.parseInt(cantidad) >0) {
			if (dif<0) {
				result = "Proceso Parcial";
			} else {
				if (dif>0) {
					result = "Diferencia";
				} else {
					if (dif==0) {
						result = "Procesado Ok";
					} 
				}
			}
		} else {
			result = "#N/A";
		}
		return result;
	}

}
