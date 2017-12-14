/**
 *@name Consulta.java
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
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
//import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipOutputStream;
/**
 * @description 
 */
public class EjecutarBatchJobOmnicanal2 {
	
	private static final int DIFF_HOY_FECHA_INI = 1;
	private static final int DIFF_HOY_FECHA_FIN = 1;
	//private static final int FORMATO_FECHA_0 = 0;
	//private static final int FORMATO_FECHA_1 = 1;
	//private static final int FORMATO_FECHA_3 = 3;
	private static final int FORMATO_FECHA_4 = 4;
	
	private static final String RUTA_ENVIO = "\\\\172.18.150.41/datamart";// \\172.18.150.41\datamart windows
	//private static final String RUTA_ENVIO = "C:/Share/Inbound/OMNICANAL";
	

	private static BufferedWriter bw;
	private static String path;
	
	//private static NotificacionServices	notificacionServices;

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
		Connection dbconnOracle = crearConexionOracle();
		File file1              = null;
		File file2              = null;
		//File file3              = null;
		
		BufferedWriter bw       = null;
		BufferedWriter bw2       = null;
		//BufferedWriter bw3       = null;

		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		StringBuffer sb         = null;
		StringBuffer sb2         = null;
		
		String sFechaIni        = null;
		String sFechaFin        = null;
		String sFechaFin2        = null;
		
		byte[] buffer = new byte[1024];
		
		
		//boolean log = false;
		//File archivo = null;
		//File dir = null;
		
		
		
		Date now2 = new Date();
		SimpleDateFormat ft2 = new SimpleDateFormat ("dd/MM/YY hh:mm:ss");
		String currentDate2 = ft2.format(now2);
		info("Inicio Programa: " + currentDate2 + "\n");

		try {

			try {

				sFechaIni = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_INI);
				sFechaFin = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);
				sFechaFin2 = restarDias2(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);


				//sFechaIni = "29-03-2017";
				//sFechaFin = "29-03-2017";
				info("sFechaIni: " + sFechaIni + "\n");
				info("sFechaFin: " + sFechaFin + "\n");
				info("sFechaFin2: " + sFechaFin2 + "\n");
			}
			catch (Exception e) {

				info("error: " + e);
			}
			//file1                   = new File(path + "/" + sFechaIni + "_" + sFechaFin + ".txt");
			//file1                   = new File(RUTA_ENVIO + "/" + sFechaIni + "_" + sFechaFin + ".txt");
			file1                   = new File(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".dat");
			file2                   = new File(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".ctr");
			//file3                   = new File(RUTA_ENVIO + "/" + sFechaIni + "_" + sFechaFin + ".txt");

			sb = new StringBuffer();
			
			
			sb.append("SELECT ' ' as ASNID ");
			sb.append(",PO.ext_purchase_orders_id as SHIPMENT_ID ");
			sb.append(",o.O_FACILITY_ALIAS_ID     as Origen");
			sb.append(",NVL(o.D_FACILITY_ALIAS_ID,'CLIENTE')     as Destino ");
			sb.append(", PO.tc_purchase_orders_id as BILL_OF_LADING_NUMBER  ");
			sb.append(",ot.order_type as BILL_OF_LADING_NUMBER_1 ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.tc_po_line_id          as Linea_PO ");
			sb.append(",sum(POL.allocated_qty)          as Cant_Desc_SKU ");
			sb.append(",'' LPN_ID ");
			sb.append(",PO.created_dttm            as Fecha ");
			sb.append(",'' MANIF_NBR ");
			sb.append(",POS.description            as Est_Orden   ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",CASE  WHEN POL.order_fulfillment_option = '01' THEN 'BOPIS' WHEN POL.order_fulfillment_option = '02' THEN 'CLICK AND COLLECT' ELSE 'DESP.DOM.' END AS Bopis_CC ");
			sb.append("FROM CA14.purchase_orders po  ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL   ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos  ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID  AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2  ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb.append("INNER JOIN CA14.DO_status sts   ON o.do_status = sts.order_status    ");

			sb.append("WHERE ot.order_type_id in(6)  ");
			sb.append("AND PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND PO.created_dttm >= to_date('");
			sb.append(sFechaIni);
			//sb.append("16-08-2017");
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND PO.created_dttm <= to_date('");
			sb.append(sFechaFin);
			//sb.append("16-08-2017");
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");

			sb.append(" AND o.D_FACILITY_ALIAS_ID IS NULL ");
			sb.append(" AND pol.purchase_orders_line_status <> 940 ");
			sb.append("GROUP BY ");
			sb.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb.append(" UNION ");
			sb.append("SELECT ' ' as ASNID ");
			sb.append(",PO.ext_purchase_orders_id as SHIPMENT_ID ");
			sb.append(",o.O_FACILITY_ALIAS_ID     as Origen ");
			sb.append(",NVL(o.D_FACILITY_ALIAS_ID,'CLIENTE')     as Destino ");
			sb.append(", PO.tc_purchase_orders_id as BILL_OF_LADING_NUMBER  ");
			sb.append(",ot.order_type as BILL_OF_LADING_NUMBER ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.tc_po_line_id          as Linea_PO ");
			sb.append(",sum(POL.allocated_qty)          as Cant_Desc_SKU ");
			sb.append(",'' LPN_ID ");
			sb.append(",PO.created_dttm            as Fecha ");
			sb.append(",'' MANIF_NBR ");
			sb.append(",POS.description            as Est_Orden   ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",CASE  WHEN POL.order_fulfillment_option = '01' THEN 'BOPIS' WHEN POL.order_fulfillment_option = '02' THEN 'CLICK AND COLLECT' ELSE 'DESP.DOM.' END AS Bopis_CC ");
			//sb.append(",'NO' as venta_empresa ");
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL  ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic  ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			
			sb.append("LEFT JOIN CA14.orders o  ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id  AND o.IS_CANCELLED = 0 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND ot.order_type_id in(22,42)  "); // --PickUP C&C
			sb.append("AND POL.order_fulfillment_option = '02' ");
			sb.append("AND PO.created_dttm >= to_date('");
			sb.append(sFechaIni);
			//sb.append("15-08-2017");
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND PO.created_dttm <= to_date('");
			sb.append(sFechaFin);
			//sb.append("15-08-2017");
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND o.O_FACILITY_ALIAS_ID <> o.D_FACILITY_ALIAS_ID "); //-PICKUP
			sb.append("AND pol.purchase_orders_line_status <> 940 ");
			sb.append("GROUP BY ");
			sb.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb.append(" UNION ");
			sb.append("SELECT ' ' as ASNID ");
			sb.append(",PO.ext_purchase_orders_id as SHIPMENT_ID ");
			sb.append(",o.O_FACILITY_ALIAS_ID     as Origen ");
			sb.append(",NVL(o.D_FACILITY_ALIAS_ID,'CLIENTE')     as Destino ");
			sb.append(", PO.tc_purchase_orders_id as BILL_OF_LADING_NUMBER   ");
			sb.append(",ot.order_type as BILL_OF_LADING_NUMBER_1 ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.tc_po_line_id          as Linea_PO ");
			sb.append(",sum(POL.allocated_qty)          as Cant_Desc_SKU ");
			sb.append(",'' LPN_ID ");
			sb.append(",PO.created_dttm            as Fecha ");
			sb.append(",'' MANIF_NBR ");
			sb.append(",POS.description            as Est_Orden ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",CASE WHEN POL.order_fulfillment_option = '01' THEN 'BOPIS' WHEN POL.order_fulfillment_option = '02' THEN 'CLICK AND COLLECT' ELSE 'DESP.DOM.' END AS Bopis_CC ");
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND ot.order_type_id in(42)  "); // PickUP BOPIS
			sb.append("AND POL.order_fulfillment_option = '01' ");
			sb.append("AND PO.created_dttm >= to_date('");
			sb.append(sFechaIni);
			//sb.append("15-08-2017");
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND PO.created_dttm <= to_date('");
			sb.append(sFechaFin);
			//sb.append("15-08-2017");
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND pol.purchase_orders_line_status <> 940  ");
			sb.append("GROUP BY ");
			sb.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			

			
			info("Query : " + sb + "\n");
			
			pstmt = dbconnOracle.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			bw = new BufferedWriter(new FileWriter(file1));
			//bw3 = new BufferedWriter(new FileWriter(file3));

			/*
			bw.write("ASNID|");
			bw.write("SHIPMENT_ID|");
			bw.write("ORIGEN|");
			bw.write("DESTINO|");
			bw.write("BILL_OF_LADING_NUMBER|");
			bw.write("BILL_OF_LADING_NUMBER_1|");
			bw.write("N_ORDEN_DISTRIBU|");
			bw.write("SKU|");
			bw.write("LINEA_PO|");
			bw.write("CANT_DESC_SKU|");
			bw.write("LPN_ID|");
			bw.write("FECHA|");
			bw.write("MANIF_NBR|");
			bw.write("EST_ORDEN|");
			bw.write("ESTADO_DE_LINEA|");
			bw.write("BOPIS_CC\n");
			*/
			
			
			sb = new StringBuffer();
			int count = 0;
			while (rs.next()){
				count = count +1 ;
				
				
				//bw.write(rs.getString("ASNID") + "|");
				bw.write(rs.getString("SHIPMENT_ID") + "|");
				bw.write(rs.getString("SHIPMENT_ID") + "|");
				bw.write(rs.getString("ORIGEN") + "|");
				bw.write(rs.getString("DESTINO") + "|");
				bw.write(rs.getString("BILL_OF_LADING_NUMBER") + "|");
				bw.write(rs.getString("BILL_OF_LADING_NUMBER_1") + "|");
				bw.write(rs.getString("N_ORDEN_DISTRIBU") + "|");
				bw.write(rs.getString("SKU") + "|");
				bw.write(rs.getString("LINEA_PO") + "|");
				bw.write(rs.getString("CANT_DESC_SKU") + "|");
				bw.write(rs.getString("LPN_ID") + "|");
				bw.write(formatDate(rs.getTimestamp("FECHA"), FORMATO_FECHA_4) + "|");
				bw.write(rs.getString("MANIF_NBR") + "|");
				bw.write(rs.getString("EST_ORDEN") + "|");
				bw.write(rs.getString("ESTADO_DE_LINEA") + "|");
				bw.write(rs.getString("BOPIS_CC") + "\n");
			}
			bw.write(sb.toString());
			
			
			Thread.sleep(120);
			info("Pausa de (60 seg)");
			
			
			sb2 = new StringBuffer();
			
			sb2.append("SELECT SUM(1) as total ");
			sb2.append("FROM ( ");
			
			
			sb2.append("SELECT SUM(1) as total  ");
			sb2.append("FROM CA14.purchase_orders po  ");
			sb2.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL   ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos  ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb2.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb2.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb2.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID  AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb2.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos2  ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb2.append("INNER JOIN CA14.DO_status sts   ON o.do_status = sts.order_status    ");

			sb2.append("WHERE ot.order_type_id in(6)  ");
			sb2.append("AND PO.is_purchase_orders_confirmed = '1' ");
			sb2.append("AND PO.created_dttm >= to_date('");
			sb2.append(sFechaIni);
			//sb2.append("16-08-2017");
			sb2.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND PO.created_dttm <= to_date('");
			sb2.append(sFechaFin);
			//sb2.append("16-08-2017");
			sb2.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");

			sb2.append(" AND o.D_FACILITY_ALIAS_ID IS NULL ");
			sb2.append(" AND pol.purchase_orders_line_status <> 940 ");
			sb2.append("GROUP BY ");
			sb2.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb2.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb2.append(" UNION ALL ");
			sb2.append("SELECT SUM(1) as total ");
			sb2.append("FROM CA14.purchase_orders po ");
			sb2.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL  ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb2.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb2.append("INNER JOIN CA14.item_cbo ic  ON ic.item_id = POL.SKU_ID ");
			sb2.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			
			sb2.append("LEFT JOIN CA14.orders o  ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id  AND o.IS_CANCELLED = 0 ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb2.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb2.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb2.append("AND ot.order_type_id in(22,42)  "); // --PickUP C&C
			sb2.append("AND POL.order_fulfillment_option = '02' ");
			sb2.append("AND PO.created_dttm >= to_date('");
			sb2.append(sFechaIni);
			//sb2.append("15-08-2017");
			sb2.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND PO.created_dttm <= to_date('");
			sb2.append(sFechaFin);
			//sb2.append("15-08-2017");
			sb2.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND o.O_FACILITY_ALIAS_ID <> o.D_FACILITY_ALIAS_ID "); //-PICKUP
			sb2.append("AND pol.purchase_orders_line_status <> 940 ");
			sb2.append("GROUP BY ");
			sb2.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb2.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb2.append(" UNION ALL ");
			sb2.append("SELECT SUM(1) as total ");
			sb2.append("FROM CA14.purchase_orders po ");
			sb2.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb2.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb2.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb2.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb2.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb2.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb2.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb2.append("AND ot.order_type_id in(42)  "); // PickUP BOPIS
			sb2.append("AND POL.order_fulfillment_option = '01' ");
			sb2.append("AND PO.created_dttm >= to_date('");
			sb2.append(sFechaIni);
			//sb2.append("15-08-2017");
			sb2.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND PO.created_dttm <= to_date('");
			sb2.append(sFechaFin);
			//sb2.append("15-08-2017");
			sb2.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND pol.purchase_orders_line_status <> 940  ");
			sb2.append("GROUP BY ");
			sb2.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb2.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			sb2.append(") ");
			

			
			info("Query2 : " + sb2 + "\n");
			
			pstmt2 = dbconnOracle.prepareStatement(sb2.toString());
			ResultSet rs2 = pstmt2.executeQuery();
			bw2 = new BufferedWriter(new FileWriter(file2));
			/*
			bw2.write("chi;");
			bw2.write("paris;");
			bw2.write("0;");
			bw2.write("EOM;");
			bw2.write("ctldet;");
			bw2.write("20170313;");
			bw2.write("11;");
			bw2.write("165;");
			bw2.write("0\n");
			*/
			

			sb2 = new StringBuffer();
			while (rs2.next()){
				
				
				//bw2.write(rs2.getString("ASNID") + "|");
				bw2.write("chi" + "|");
				bw2.write("paris" + "|");
				bw2.write("0" + "|");
				bw2.write("EOM" + "|");
				bw2.write("ctldet" + "|");
				bw2.write(sFechaFin2 + "|");
				bw2.write("por definir" + "|");
				bw2.write(rs2.getString("total") + "|");
				bw2.write("por definir" + "\n");


				
			}
			bw2.write(sb2.toString());
			
			
			Thread.sleep(120);
			info("Pausa de (120 seg)");
			
			
			
			
			
			
			
			
			info("Archivos creados." + "\n");
		}
		catch (Exception e) {

			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnOracle, pstmt, bw);
			cerrarTodo(dbconnOracle, pstmt2, bw2);

		}
		
		
		
		try{
			
			
			file1                   = new File(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".dat");
			

			FileOutputStream fos = new FileOutputStream(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".zip");
    		ZipOutputStream zos = new ZipOutputStream(fos);
    		ZipEntry ze= new ZipEntry(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".dat");
    		zos.putNextEntry(ze);
    		FileInputStream in = new FileInputStream(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".dat");
			
    		int len;
    		while ((len = in.read(buffer)) > 0) {
    			zos.write(buffer, 0, len);
    		}
    		in.close();
    		zos.closeEntry();
    		//remember close it
    		zos.close();
    		fos.close();

    		System.out.println("Done");

    	} catch(IOException ex){
    	   ex.printStackTrace();
    	}
		
		
		
		try {
			Thread.sleep(120);
			eliminarArchivo(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + "11" + ".dat");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		//boolean log = false;
		//File archivo = null;
		//File dir = null;
		//String[] ficheros = null;
	    //BufferedWriter bw       = null;
	    
		/*
		try
		{
			InicializarMIS inicializarMIS = new InicializarMIS();
			inicializarMIS.init();
			notificacionServices = new NotificacionServices();
			
			
			// se buscan los correos asociados a esta transaccion
			//List<String> list = getUtilDao().findCorreos(Transacciones.COPIAR_ARCHIVOS);
			List<String> list = new ArrayList<String>();
			list.add("jose.floyd.jose@gmail.com");
			//list.add("alexis.morales@cencosud.cl");
			destinatarios = new String[list.size()];
			destinatarios = list.toArray(destinatarios);
			info("list.size() " +list.size());
			info("destinatarios " +destinatarios);
			info("============="+InicializarMIS.getPropiedad(Constantes.SCHEDULE_DIA));
			
			//Runtime.getRuntime().exec("C:\\PROYECTOS\\C8INVERSIS\\PROC_INV.bat");
			//Runtime.getRuntime().exec(InicializarMIS.getPropiedad(Constantes.KEY_FILE_BATCH) + " "+InicializarMIS.getPropiedad(Constantes.SERVER));
			
			//Verifico si se genero un archivo en la ruta ERR, si se genera un 
			//archivo es porque hubo algun error en el proceso
			
		    	//se envia la notificacion
		    	//No se esta adjuntando el archivo .log
		    	info("adjunto="+adjunto);
		    	info("Transacciones.COPIAR_ARCHIVOS="+Transacciones.COPIAR_ARCHIVOS);
		    	info("destinatarios="+destinatarios);
		    	info("Constantes.ASUNTO_CARGA_ARCHIVOS="+Constantes.ASUNTO_CARGA_ARCHIVOS);
		    	info("nicializarMIS.getPropiedad(Constantes.NOTIFICACION6)="+InicializarMIS.getPropiedad(Constantes.NOTIFICACION6));
		    	info("Constantes.ASUNTO_CARGA_ARCHIVOS="+Constantes.ASUNTO_CARGA_ARCHIVOS);
		    	info("notificacionServices="+notificacionServices);
		    	notificacionServices.enviarCorreo(Transacciones.COPIAR_ARCHIVOS, destinatarios, "---", Constantes.ASUNTO_CARGA_ARCHIVOS, adjunto, "txt");

		    			    
		} catch (Exception e)
		{
			info("111111111=");
			// Se hay una excepcion de cualquier tipo se notifica que hubo algun error interno
			info(e.getMessage());
			
		
			info("No Se envio Notificacion de correo");
		}
		finally
		{
			if (null != fr && br != null)
			{
				try
				{
					fr.close();
					br.close();
				} catch (IOException e)
				{
					info("Algun error cerrando objetos de lectura de archivos");
					e.printStackTrace();
				}
			}
		}
		*/
		
		info("Fin Programa: " + currentDate2 + "\n");
	}
	
	public static void eliminarArchivo(String archivo){

	     File fichero = new File(archivo);
	   
	     if(fichero.delete()){

	          System.out.println("archivo eliminado");
	    
	     } else {
	    	 System.out.println("archivo no eliminado");
	     }

	}  

	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			//Shareplex
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");
			
			//El servidor g500603sv0zt corresponde a Producción. por el momento
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP","ca14","Manhattan1234");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	}



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

			System.out.println("Exception:" + e.getMessage());
		}
	}


	private static String restarDias2(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}



	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "dd-MM-yyyy";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}

	private static String formatDate(Date fecha, int iOptFormat) {

		String sFormatedDate = null;
		String sFormat = null;

		try {

			SimpleDateFormat df = null;

			switch (iOptFormat) {

			case 0:
				sFormat = "dd/MM/yy HH:mm:ss,SSS";
				break;
			case 1:
				sFormat = "dd/MM/yy";
				break;
			case 2:
				sFormat = "dd/MM/yy HH:mm:ss";
				break;
			case 3:
				sFormat = "yyyy-MM-dd HH:mm:ss,SSS";
				break;
				
			case 4:
				sFormat = "yyyyMMdd";
				break;
			}
			df = new SimpleDateFormat(sFormat);
			sFormatedDate = df.format(fecha != null ? fecha:new Date(0));

			if (iOptFormat == 0 && sFormatedDate != null) {

				sFormatedDate = sFormatedDate + "000000";
			}
		}
		catch (Exception e) {

			info("[formatDate]Exception:"+e.getMessage());
		}
		return sFormatedDate;
	}

}
