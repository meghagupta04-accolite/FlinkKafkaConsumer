package ftpconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

class FtpClient {

	private String server;
	private int port;
	private String user;
	private String password;
	private FTPClient ftp;


	public static void main(String[] args) {
		FtpClient ftpClient = new FtpClient("test.rebex.net", 21, "demo", "password");
		try {
			ftpClient.open();
			ftpClient.listFiles("pub/example");
			System.out.println("----------------------------------------");
          //  ftpClient.downloadFTPFile("pub/example/readme.txt", "/");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public FtpClient(String server, int port, String user, String password) {
		super();
		this.server = server;
		this.port = port;
		this.user = user;
		this.password = password;
	}

	public void open() throws IOException {
		ftp = new FTPClient();

		ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));

		ftp.connect(server, port);
		int reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			throw new IOException("Exception in connecting to FTP Server");
		}

		ftp.login(user, password);
	}

	public void close() throws IOException {
		ftp.disconnect();
	}

	// Method to upload the File on the FTP Server
	public void uploadFTPFile(String localFileName, String fileName, String hostDir) throws Exception
	{
		try {
			InputStream input = new FileInputStream(new File(localFileName));

			this.ftp.storeFile(hostDir + fileName, input);
		}
		catch(Exception e){

		}
	}

	// Download the FTP File from the FTP Server
	public void downloadFTPFile(String source, String destination) {
		/*try (FileOutputStream fos = new FileOutputStream(destination)) {
			System.out.println(this.ftp.retrieveFile(source, fos));
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}

	// list the files in a specified directory on the FTP
	public boolean listFTPFiles(String directory, String fileName) throws IOException {
		// lists files and directories in the current working directory
		boolean verificationFilename = false;        
		FTPFile[] files = ftp.listFiles(directory);
		for (FTPFile file : files) {
			String details = file.getName();                
			System.out.println(details);            
			if(details.equals(fileName))
			{
				System.out.println("Correct Filename");
				verificationFilename=details.equals(fileName);
				//	assertTrue("Verification Failed: The filename is not updated at the CDN end.",details.equals(fileName));                
			}
		}  

		return verificationFilename;
	}
	
	Collection<String> listFiles(String path) throws IOException {
		FTPFile[] files = ftp.listFiles(path);
		ArrayList<String> filesList = new ArrayList<String>();
		for (FTPFile file : files) {
			filesList.add(file.getName());
			System.out.println(file.getName());
		}
		return filesList;
	}
}
