package dbsas2;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*BPlusTree class to store various details regarding each block of the file like,
 *  the key values and pointers to connecting blocks*/
@SuppressWarnings("serial")
class BPlusTree implements Serializable {
	public List<String> key; //ArrayList to store the key of the records
	public List<BPlusTree> blockPtr; //ArrayList to store the block pointers of intermediate blocks to corresponding blocks
	public BPlusTree parent; //stores the pointer to the parent of the current block
	public List<Long> offsetval; //ArrayList to store the offset of the records present in the input text data file
	public BPlusTree rightBlockPtr; //pointer of right block of the current node 
	public BPlusTree leftBlockPtr; //pointer of right block of the current node 
	public boolean isLeafBlock; //determines if the current block is a leaf block or intermediate block
	
	public BPlusTree() {
		this.key = new ArrayList<String>();
		this.blockPtr = new ArrayList<BPlusTree>();
		this.parent = null;
		this.offsetval = new ArrayList<Long>();
		this.rightBlockPtr = null;
		this.leftBlockPtr = null;
		this.isLeafBlock = false;
	}
}

public class treeload {
	
	static BPlusTree root;//root block of the index file
	static int MaxBlockSize;//max block size of each block
	static int splitIndex;//index at which we must split the current block once it reaches the maxBlockSize
	
	/**
	 * Create the index file using the B plus tree algorithm used to search records faster
	 *
	 * @param inputFile : path to the input text data file which needs to be indexed for faster access
	 * @param indexFile : path where the index file needs to be generated
	 * @param keyLength : length of the key to be considered while preparing the index file
	 * */
	public static void createIndex(String inputFile, String indexFile, int keyLength) throws IOException{
		
		long offset = 0l;// to calculate the offset of the records in the text input file
		MaxBlockSize = (1024 - keyLength) / keyLength + 8 ; // assuming each block of 1024 bytes and offset of long data type, hence 8 bytes
		splitIndex = (MaxBlockSize%2==0)?(MaxBlockSize/2)-1 : MaxBlockSize/2;
		
		//read the input text file
		File file = new File(inputFile); 
        FileInputStream fileStream = new FileInputStream(file); 
        InputStreamReader input = new InputStreamReader(fileStream); 
        BufferedReader reader = new BufferedReader(input);
		String line="",key = "";
		
		while((line = reader.readLine()) != null){ //read each line of the input text file
			key = line.substring(0,keyLength); //extract key upto the specified keyLength from each line to insert in the index file
			insertRecord(root,key, offset); // insert each new key offset pair of the record read from input file to the index file
			offset += line.length() + 2; //add 2 to each offset after adding line.length() for "\n" i.e. newline charaters
		}
		reader.close(); 
		writefile(keyLength, inputFile, indexFile);
	
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// check for correct number of arguments
        if (args.length != constants.DBQUERY_ARG_COUNT) {
            System.out.println("Error: Incorrect number of arguments were input");
            return;
        }

        
        int pageSize = Integer.parseInt(args[constants.DBQUERY_PAGE_SIZE_ARG]);
        String outputFileName = "tree." + pageSize;
        String datafile = "heap." + pageSize;
        long startTime = 0;
        long finishTime = 0;
        int numRecordsLoaded = 0;
        int numberOfPagesUsed = 0;
        int numBytesInOneRecord = constants.TOTAL_SIZE;
        int numBytesInSdtnameField = constants.STD_NAME_SIZE;
        int numBytesIntField = Integer.BYTES;
        int numRecordsPerPage = pageSize/numBytesInOneRecord;
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        byte[] page = new byte[pageSize];
        FileInputStream inStream = null;
        
        BufferedReader reader = null;
        FileOutputStream outputStream = null;
        ByteArrayOutputStream byteOutputStream = null;
        DataOutputStream dataOutput = null;

        try {
        	reader = new BufferedReader(new FileReader(datafile));
            outputStream = new FileOutputStream(outputFileName, true);
            byteOutputStream = new ByteArrayOutputStream();
            dataOutput = new DataOutputStream(byteOutputStream);
            
            inStream = new FileInputStream(datafile);
            int numBytesRead = 0;
            startTime = System.nanoTime();
            // Create byte arrays for each field
            byte[] sdtnameBytes = new byte[numBytesInSdtnameField];
            byte[] idBytes = new byte[constants.ID_SIZE];
            byte[] dateBytes = new byte[constants.DATE_SIZE];
            byte[] yearBytes = new byte[constants.YEAR_SIZE];
            byte[] monthBytes = new byte[constants.MONTH_SIZE];
            byte[] mdateBytes = new byte[constants.MDATE_SIZE];
            byte[] dayBytes = new byte[constants.DAY_SIZE];
            byte[] timeBytes = new byte[constants.TIME_SIZE];
            byte[] sensorIdBytes = new byte[constants.SENSORID_SIZE];
            byte[] sensorNameBytes = new byte[constants.SENSORNAME_SIZE];
            byte[] countsBytes = new byte[constants.COUNTS_SIZE];

            // until the end of the binary file is reached
            while ((numBytesRead = inStream.read(page)) != -1) {
                // Process each record in page
                for (int i = 0; i < numRecordsPerPage; i++) {

                    // Copy record's SdtName (field is located at multiples of the total record byte length)
                    System.arraycopy(page, (i*numBytesInOneRecord), sdtnameBytes, 0, numBytesInSdtnameField);

                    // Check if field is empty; if so, end of all records found (packed organisation)
                    if (sdtnameBytes[0] == 0) {
                        // can stop checking records
                        break;
                    }

                    
                  
                    
                    
                        /*
                         * Fixed Length Records (total size = 112 bytes):
                         * SDT_NAME field = 24 bytes, offset = 0
                         * id field = 4 bytes, offset = 24
                         * date field = 8 bytes, offset = 28
                         * year field = 4 bytes, offset = 36
                         * month field = 9 bytes, offset = 40
                         * mdate field = 4 bytes, offset = 49
                         * day field = 9 bytes, offset = 53
                         * time field = 4 bytes, offset = 62
                         * sensorid field = 4 bytes, offset = 66
                         * sensorname field = 38 bytes, offset = 70
                         * counts field = 4 bytes, offset = 108
                         *
                         * Copy the corresponding sections of "page" to the individual field byte arrays
                         */
                    	System.arraycopy(page, i*numBytesInOneRecord, sdtnameBytes, 0, constants.STD_NAME_SIZE);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.ID_OFFSET), idBytes, 0, numBytesIntField);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.DATE_OFFSET), dateBytes, 0, constants.DATE_SIZE);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.YEAR_OFFSET), yearBytes, 0, numBytesIntField);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.MONTH_OFFSET), monthBytes, 0, constants.MONTH_SIZE);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.MDATE_OFFSET), mdateBytes, 0, numBytesIntField);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.DAY_OFFSET), dayBytes, 0, constants.DAY_SIZE);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.TIME_OFFSET), timeBytes, 0, numBytesIntField);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.SENSORID_OFFSET), sensorIdBytes, 0, numBytesIntField);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.SENSORNAME_OFFSET), sensorNameBytes, 0, constants.SENSORNAME_SIZE);
                        System.arraycopy(page, ((i*numBytesInOneRecord) + constants.COUNTS_OFFSET), countsBytes, 0, numBytesIntField);
                        
                        
                     

                        // Write bytes to data output stream
                        dataOutput.write(sdtnameBytes);
                        dataOutput.write(idBytes);
                        dataOutput.write(dateBytes);
                        dataOutput.write(yearBytes);
                        dataOutput.write(monthBytes);
                        dataOutput.write(mdateBytes);
                        dataOutput.write(dayBytes);
                        dataOutput.write(timeBytes);
                        dataOutput.write(sensorIdBytes);
                        dataOutput.write(sensorNameBytes);
                        dataOutput.write(countsBytes);

                        numRecordsLoaded++;
                        // check if a new page is needed
                        if (numRecordsLoaded % numRecordsPerPage == 0) {
                            dataOutput.flush();
                            // Get the byte array of loaded records, copy to an empty page and writeout
                            
                            byte[] records = byteOutputStream.toByteArray();
                            int numberBytesToCopy = byteOutputStream.size();
                            System.arraycopy(records, 0, page, 0, numberBytesToCopy);
                            writeOut(outputStream, page);
                            numberOfPagesUsed++;
                            byteOutputStream.reset();
                        }

                        
                    
                }
                
             // At end of csv, check if there are records in the current page to be written out
                if (numRecordsLoaded % numRecordsPerPage != 0) {
                    dataOutput.flush();
                    
                    byte[] records = byteOutputStream.toByteArray();
                    int numberBytesToCopy = byteOutputStream.size();
                    System.arraycopy(records, 0, page, 0, numberBytesToCopy);
                    writeOut(outputStream, page);
                    numberOfPagesUsed++;
                    byteOutputStream.reset();
                }
            }

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
	
	public static void writeOut(FileOutputStream stream, byte[] byteArray)
            throws FileNotFoundException, IOException {

        stream.write(byteArray);
    }

    // Returns a whitespace padded string of the same length as parameter int length
    public static String getStringOfLength(String original, int length) {

        int lengthDiff = length - original.length();

        // Check difference in string lengths
        if (lengthDiff == 0) {
            return original;
        }
        else if (lengthDiff > 0) {
            // if original string is too short, pad end with whitespace
            StringBuilder string = new StringBuilder(original);
            for (int i = 0; i < lengthDiff; i++) {
                string.append(" ");
            }
            return string.toString();
        }
        else {
            // if original string is too long, shorten to required length
            return original.substring(0, length);
        }
    }

}
