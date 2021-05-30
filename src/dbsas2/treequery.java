package dbsas2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;

public class treequery {
	
	/**
	 * Get Metadata like text file name and key length used from the index file
	 *
	 * @param indexFile : path where the index file is generated
	 * @return the string array having the textfilename and keylength fetched from the index file
	 * */
	public static String[] getMetaData(String indexFile) throws IOException{

		RandomAccessFile randomIndexFile = new RandomAccessFile(indexFile, "r");
		//fetch the name of the input text file from the metadata we added in the index file
		randomIndexFile.seek(0);
		byte[] b = new byte[256];
		randomIndexFile.read(b);
		String textFileName = (new String(b)).trim();
		//fetch the key length from the metadata we added in the index file 
		randomIndexFile.seek(256);
		byte[] keyLengthBuffer = new byte[4];
		randomIndexFile.read(keyLengthBuffer);
		String keyLength = new String(keyLengthBuffer);
		keyLength = keyLength.trim();
		randomIndexFile.close();
		
		String[] metData = {textFileName,keyLength};
		return metData;
	}
	
	/**
	 * To find if the record exists in the file and list the subsequent records,
	 * if operation chosen is list and number of records to list is specified
	 *
	 * @param indexFile : path where the index file is generated
	 * @param key : key of the record to be searched in the index file
	 * @param numOfRecords : Number of subsequent records to be listed
	 * @param block : root block object of BPlusTree type to start the search
	 * @param insertFind : flag to find the record but not print the result in console
	 * @return : 0 if record not found and 1 if record found
	 * */
	public static int listRecords(String indexFile, String key, BPlusTree block, boolean insertFind) throws IOException, ClassNotFoundException{
		
		String[] metaData = getMetaData(indexFile);
		
		String textFileName = metaData[0];
		int keyLength = Integer.parseInt(metaData[1]);
		int currentKeyLength = key.length();
		
		//	if specified keyLength is small, then pad with blank to match the keylength used in the index file,
		//	if specified keyLength is large, then trim to match the keylength used in the index file
		if(currentKeyLength<keyLength){
			for(int i = 0;i<keyLength-currentKeyLength;i++){
				key+=" ";
			}
		}
		else{
			key = key.substring(0, keyLength);
		}
		
		int recordsListed = 0;//counter to count the number of records listed
		
		for(int i = 0;i<block.key.size();i++){
			if(!block.isLeafBlock){
				if(key.compareTo(block.key.get(i))<0){
					return listRecords(indexFile, key,  block.blockPtr.get(i),insertFind);
					 
				}
				else{
					if (i < block.key.size() - 1) {
						continue;
					}

					else if (i == block.key.size() - 1) {
						if (!block.isLeafBlock && block.blockPtr.get(i + 1) != null) {
							return listRecords(indexFile, key, block.blockPtr.get(i + 1),insertFind);
						}
					}
				}
			}
			//We have traversed and reached the leaf block where we can find the key
			else{
				
				if(block.key.indexOf(key)==-1){
					if(!insertFind){
						System.out.println("Given key doesn't exist.");
					}
					
					
				}
				while(block!=null){
					for(int j = 0;j<block.key.size();j++){
						if(key.compareTo(block.key.get(j))>0){
							if (j < block.key.size() - 1) {
								continue;
							}
							else{
								block = block.blockPtr.get(j+1);
							}
						}
						else{
							
							long offsetVal = block.offsetval.get(j);
							
							if(!insertFind){
								readTextFileRecord(textFileName, offsetVal);
							}
							
							
							recordsListed++;
						}
					}
					block = block.rightBlockPtr;
				}
			}
		}
		return 0;
	}
	
	/**
	 * Helper function to fetch the root block from the generated index file
	 *
	 * @param indexFile : path where the index file is generated
	 * @return the root block
	 * */
	public static BPlusTree getRoot(String indexFile) throws IOException, ClassNotFoundException{
		
		FileInputStream fin = new FileInputStream(indexFile);
		FileChannel fc = fin.getChannel();
		fc.position(1025l);// root block starts from 1025th byte as first 1024 bytes reserved for storing metadata
		ObjectInputStream ois = new ObjectInputStream(fin);
		BPlusTree root = (BPlusTree) ois.readObject();
		ois.close();
		
		return root;
	}
	
	/**
	 * To read records from the input tree load file using the offset read from the generated index file 
	 *
	 * @param textFileName : path of the input tree load file
	 * @param offsetVal : offset of the required record to be read from the input tree load file
	 * */
	public static void readTextFileRecord(String textFileName, long offsetVal) throws IOException{
		
		RandomAccessFile textFile = new RandomAccessFile(textFileName, "r");
		textFile.seek(offsetVal);
		String line = textFile.readLine();
		
		System.out.println("At "+offsetVal+", record: "+line);
		textFile.close();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		// check for correct number of arguments
        if (args.length != constants.DBQUERY_ARG_COUNT) {
            System.out.println("Error: Incorrect number of arguments were input");
            return;
        }

        String text = args[0];
        int pageSize = Integer.parseInt(args[constants.DBQUERY_PAGE_SIZE_ARG]);

        String datafile = "tree." + pageSize;
        long startTime = 0;
        long finishTime = 0;
        
        FileInputStream inStream = null;
        
        try {
            inStream = new FileInputStream(datafile);
            
            startTime = System.nanoTime();
            // list all records which has the key in index file
            listRecords(datafile, text,getRoot(datafile),false);

            

            finishTime = System.nanoTime();
        }
        catch (FileNotFoundException e) {
            System.err.println("File not found " + e.getMessage());
        }
        catch (IOException e) {
            System.err.println("IO Exception " + e.getMessage());
        }
        finally {

            if (inStream != null) {
                inStream.close();
            }
        }

        long timeInMilliseconds = (finishTime - startTime)/constants.MILLISECONDS_PER_SECOND;
        System.out.println("Time taken: " + timeInMilliseconds + " ms");
	}

}
