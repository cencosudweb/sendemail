/**
 * @name Nuevo_subir.java 
 * 
 * @version 1.0 
 * 
 * @date 9 sept. 2017 
 * 
 * @author josef_000 
 * 
 * @copyright Nombre Empresa. All rights reserved.
 */
package cl.org.is.api.job;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

/**
 * @description
 *
 */
public class Nuevo_bajar {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws SocketException, UnknownHostException, IOException {
		// TODO Auto-generated method stub
		try {
			FTPClient ftpClient = new FTPClient();
			ftpClient.connect(InetAddress.getByName("aristocrata.cl"));
			ftpClient.login("aristocr", "nx37GwQp55");

			// Verificar conexi�n con el servidor.

			int reply = ftpClient.getReplyCode();

			System.out.println("Respuesta recibida de conexi�n FTP:" + reply);

			if (FTPReply.isPositiveCompletion(reply)) {
				System.out.println("Conectado Satisfactoriamente");
			} else {
				System.out.println("Imposible conectarse al servidor");
			}

			// Verificar si se cambia de direcotirio de trabajo

			ftpClient.changeWorkingDirectory("/public_html");// Cambiar directorio de
													// trabajo
			System.out.println("Se cambi� satisfactoriamente el directorio");

			// Activar que se envie cualquier tipo de archivo

			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			//BufferedInputStream buffIn = null;
			//buffIn = new BufferedInputStream(new FileInputStream("C:\\PROYECTOS\\C8INVERSIS\\LOG\\test.txt"));// Ruta
																		// del
																		// archivo
																		// para
																		// enviar
			//ftpClient.enterLocalPassiveMode();
			//ftpClient.storeFile("test.txt", buffIn);// Ruta completa de alojamiento en
											// el FTP
			
			String archivo = "/test.txt";
			FileOutputStream stream = null;
			stream = new FileOutputStream("C:\\PROYECTOS\\C8INVERSIS\\LOG\\test.txt");
			ftpClient.retrieveFile(archivo, stream);

			stream.close(); // Cerrar envio de arcivos al FTP
			ftpClient.logout(); // Cerrar sesi�n
			ftpClient.disconnect();// Desconectarse del servidor
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.out.println("Algo malo sucedi�");
		}

	}

}
