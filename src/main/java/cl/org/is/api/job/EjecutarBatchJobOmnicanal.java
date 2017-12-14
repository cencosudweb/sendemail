/**
 *@name CopiarArchivosCuadraturaJob.java
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import org.apache.log4j.Logger;

import cl.org.is.api.constantes.Constantes;
import cl.org.is.api.constantes.Errores;
import cl.org.is.api.constantes.Transacciones;
import cl.org.is.api.services.NotificacionServices;
import cl.org.is.api.util.InicializarMIS;

/**
 * @description 
 */
public class EjecutarBatchJobOmnicanal {
	
	/** Variable que gestiona la impresion del log */
	private static Logger		logger			= Logger.getLogger( EjecutarBatchJobOmnicanal.class);
	
	private Timer		timerServiceCopiarArchivosFTPJob;
	
	public static final String	SCHEDULE_HORA		= "horaCopiarArchivosFTPJob";
	public static final String	SCHEDULE_MINUTO	= "minutoCopiarArchivosFTPJob";
	public static final String	SCHEDULE_SEGUNDO	= "segundoCopiarArchivosFTPJob";
	
	
	/** TimerService necesario para inicializar el timer*/
	
	//@EJB
	private static NotificacionServices	notificacionServices;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		crearProgramacion();
	}
	
	/**
	* metodo que crea la programacion de este job basado en la configuracion del properties
	* 
	*/
	//@PostConstruct
	public static void crearProgramacion() {
		
		
		logger.info("Inicio: crearProgramacion");
		
		removeTimer();
		try {
			executeBatch();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		logger.info("Fin: " + "crearProgramacion");
		
	}
	
	/**
	* metodo que elimina cualquier programacion existente de este job
	* 
	*/
	public static void removeTimer() {
		
		
		logger.info("Inicio: removeTimer");
		
		//Collection<Timer> timers = getTimerServiceCopiarArchivosFTPJob().getTimers();
		String timers = "11";
		if (timers != null)
		{
			//for (Timer t : timers)
			for (int i = 0; i<=1; i++)
			{
				try
				{
					//t.cancel();
					logger.info("shutDownTimer - timer \"" + "" + "\" canceled.");
					
				} catch (Exception e)
				{
					logger.error(Errores.MENSAJE_ERROR_JOB);
					logger.error(Errores.EXCEPTION_MESSAGE + e.getMessage());
					logger.error(Errores.EXCEPTION_LOCALIZED + e.getLocalizedMessage());
					e.printStackTrace();
				}				
			}
		}
		
		logger.info("Fin:" + "removeTimer");
	}
	
	/**
	* metodo que se ejecuta automaticamente dependiendo de la programacion (fecha, hora, min, etc.) que tenga el metodo
	* 
	*/
	public void execute(Timer timer) {		
		
		logger.info("Inicio: execute");
		
		try
		{
			// Para verificar si el servidor donde corre la aplicacion
			// ejecutara el Job
			//TODO desabilitado la ejecucion
			if (!InicializarMIS.getPropiedad(Constantes.EJECUCION_JOB).equals(Constantes.CODIGO_JOB))
			{
				//executeBatch();
			}
			
		} catch (Exception e)
		{
			logger.error(Errores.MENSAJE_ERROR_JOB);
			logger.error(Errores.EXCEPTION_MESSAGE + e.getMessage());
			logger.error(Errores.EXCEPTION_LOCALIZED + e.getLocalizedMessage());
			e.printStackTrace();
		}
		
		logger.info("Fin:" + "execute");
	}
	
	/**
	* metodo que ejecutar el proceso batch que carga los archivos
	* @return boolean     true si se realizo correctamente el copiado y desencriptado
	 * @throws Exception 
	*/
	protected static boolean executeBatch() throws Exception {
		
		logger.info("Inicio: executeBatch");
		logger.info("Se ejecuta proceso batch");
		boolean log = false;
		File archivo = null;
		File dir = null;
		String[] destinatarios = null;
		Map<String, byte[]>	adjunto = null;
		String[] ficheros = null;
	    FileReader fr = null;
	    BufferedReader br = null;
	    //BufferedWriter bw       = null;
		
		try
		{
			InicializarMIS inicializarMIS = new InicializarMIS();
			inicializarMIS.init();
			notificacionServices = new NotificacionServices();
			
			
			// se buscan los correos asociados a esta transaccion
			//List<String> list = getUtilDao().findCorreos(Transacciones.COPIAR_ARCHIVOS);
			List<String> list = new ArrayList<String>();
			list.add("joseluis.garrido@externos-cl.cencosud.com");
			//list.add("alexis.morales@cencosud.cl");
			destinatarios = new String[list.size()];
			destinatarios = list.toArray(destinatarios);
			logger.info("list.size() " +list.size());
			logger.info("destinatarios " +destinatarios);
			logger.info("============="+InicializarMIS.getPropiedad(Constantes.SCHEDULE_DIA));
			
			//Runtime.getRuntime().exec("C:\\PROYECTOS\\C8INVERSIS\\PROC_INV.bat");
			//Runtime.getRuntime().exec(InicializarMIS.getPropiedad(Constantes.KEY_FILE_BATCH) + " "+InicializarMIS.getPropiedad(Constantes.SERVER));
			
			//Verifico si se genero un archivo en la ruta ERR, si se genera un 
			//archivo es porque hubo algun error en el proceso
			dir = new File(InicializarMIS.getPropiedad(Constantes.KEY_FILE_BATCH_ERROR));
		    	ficheros = dir.list();
		    	String msn ="";
		    	logger.info("dir " +dir);
		    	logger.info("ficheros " +ficheros);
		    	logger.info("ficheros.length " +ficheros.length);
		    	//Hubo un error en la carga de los archivos de inversis
		    	if(ficheros.length>0)
		    	{
		    		logger.info("1="+ficheros[0]);
		    		logger.info("Hubo un error en la ejecucion del proceso batch");
		    		archivo = new File (dir+"\\"+ficheros[0]);
		    	     byte[] bFile = new byte[(int) archivo.length()];
		    		adjunto = new HashMap<String, byte[]>();
		    		adjunto.put(ficheros[0], bFile);
		    		msn = InicializarMIS.getPropiedad(Constantes.NOTIFICACION5);
		    	}
		    	//Posiblemente Todo salio bien en la carga
		    	else{
		    		logger.info("2="+"");
		    		
		    		msn = InicializarMIS.getPropiedad(Constantes.NOTIFICACION4);
		    		dir = new File(InicializarMIS.getPropiedad(Constantes.KEY_FILE_BATCH_LOG));
		    		ficheros = dir.list();
		    		logger.info("ficheros[0]="+ficheros[0]);
		    		logger.info("dir="+dir);
		    		archivo = new File (dir+"\\"+ficheros[0]);		    	
			    	fr = new FileReader (archivo);
			     br= new BufferedReader(fr);
			     String linea = "";
			     
			   
			     
			     while((linea=br.readLine()) != null)
		         {	
			    	 logger.info("linea="+linea);
				    	 //if(linea.trim().equals(msn)){
				    	 if(linea.trim()!= null){	 
				    		logger.info("msn="+msn);
				    	     byte[] bFile = new byte[(int) archivo.length()];
				    		adjunto = new HashMap<String, byte[]>();
				    		adjunto.put(ficheros[0], bFile);
				    		
						//Si llega aqui es porque se ejecuto el batch bien y no hubo errores
						log = true;
				    	}  	 
		         }
		    	}
		    	//se envia la notificacion
		    	//No se esta adjuntando el archivo .log
		    	logger.info("adjunto="+adjunto);
		    	logger.info("ficheros[0]="+ficheros[0]);
		    	logger.info("Transacciones.COPIAR_ARCHIVOS="+Transacciones.COPIAR_ARCHIVOS);
		    	logger.info("destinatarios="+destinatarios);
		    	logger.info("msn="+msn);
		    	logger.info("Constantes.ASUNTO_CARGA_ARCHIVOS="+Constantes.ASUNTO_CARGA_ARCHIVOS);
		    	logger.info("nicializarMIS.getPropiedad(Constantes.NOTIFICACION6)="+InicializarMIS.getPropiedad(Constantes.NOTIFICACION6));
		    	logger.info("Constantes.ASUNTO_CARGA_ARCHIVOS="+Constantes.ASUNTO_CARGA_ARCHIVOS);
		    	logger.info("notificacionServices="+notificacionServices);
		    	//notificacionServices.enviarCorreo(Transacciones.COPIAR_ARCHIVOS, destinatarios, msn, Constantes.ASUNTO_CARGA_ARCHIVOS, adjunto, "txt");
		    	notificacionServices.enviarCorreo(Transacciones.COPIAR_ARCHIVOS, destinatarios, msn, Constantes.ASUNTO_CARGA_ARCHIVOS, adjunto, "zip");

		    			    
		} catch (Exception e)
		{
			logger.info("111111111=");
			// Se hay una excepcion de cualquier tipo se notifica que hubo algun error interno
			logger.error(e.getMessage());
			
		
			logger.info("No Se envio Notificacion de correo");
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
					logger.info("Algun error cerrando objetos de lectura de archivos");
					e.printStackTrace();
				}
			}
		}
		

		logger.info("Fin:" + "executeBatch");
		return log;
	}
	
	

	public Timer getTimerServiceCopiarArchivosFTPJob() {
		return timerServiceCopiarArchivosFTPJob;
	}

	public void setTimerServiceCopiarArchivosFTPJob(
			Timer timerServiceCopiarArchivosFTPJob) {
		this.timerServiceCopiarArchivosFTPJob = timerServiceCopiarArchivosFTPJob;
	}
	
	public static NotificacionServices getNotificacionServices() {
		
		
		return notificacionServices;
	}
	/*
	public void setNotificacionServices(NotificacionServices notificacionServices) {
		
		
		this.notificacionServices = notificacionServices;
	}
	*/
	
	

}
