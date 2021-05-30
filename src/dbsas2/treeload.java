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
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
	public static void createIndex(String inputFile, String indexFile) throws IOException{
		
		long offset = 1;// to calculate the offset of the records in the text input file
		MaxBlockSize = (1024 - 24) / 24 + 8 ; // assuming each block of 1024 bytes and offset of long data type, hence 8 bytes
		splitIndex = (MaxBlockSize%2==0)?(MaxBlockSize/2)-1 : MaxBlockSize/2;
		
		//read the input heap file
		
        FileInputStream fileStream = new FileInputStream(inputFile); 
        InputStreamReader input = new InputStreamReader(fileStream); 
        BufferedReader reader = new BufferedReader(input);
		String line="",key = "";
		
		while((line = reader.readLine()) != null){ //read each line of the input text file
			
			
			key = line.substring(0,24); //extract key up to the specified keyLength from each line to insert in the index file
			insertRecord(root,key, offset); // insert each new key offset pair of the record read from input file to the index file
			offset += line.length() + 2; //add 2 to each offset after adding line.length() for "\n"
		}
		reader.close(); 
		writefile( inputFile, indexFile);
	
	}
	
	/**
	 * Write the prepared index file object to index file
	 *
	 * @param datafilepath : path to the input text data file which needs to be indexed for faster access
	 * @param indexfilepath : path where the index file needs to be generated
	 * @param key : length of the key to be considered while preparing the index file
	 * */
	private static void writefile( String datafilepath, String indexfilepath) throws IOException {
		FileOutputStream fout = new FileOutputStream(indexfilepath);
		byte[] inputFileName = datafilepath.getBytes();
		byte[] keyLength = (24+"").getBytes();
		byte[] rootOffset = (" " + root.key.get(0)).getBytes();
		FileChannel fc = fout.getChannel();
		fc.write(ByteBuffer.wrap(inputFileName));//write the input text file name from 0-255 bytes
		fc.write(ByteBuffer.wrap(keyLength), 257l);//write the keyLength of int data type in next 4 bytes
		fc.write(ByteBuffer.wrap(rootOffset), 260l);//write the offset of the root block of long data type next
		fc.position(1025l);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(root);
		oos.close();
	}
	
	/**
	 * Insert each record read from the input text file to the block object
	 *
	 * @param block : block object of BPlusTree type to insert the record
	 * @param offset : position of the record present in the input text data file
	 * @param key : key of the record to be inserted in the index file
	 * */
	public static void insertRecord(BPlusTree block, String key, long offset)throws IOException{
		
		//insert the first record in the block
		if (block == root && (block == null || block.key.isEmpty())) {
			block.key.add(key);
			block.offsetval.add((Long) offset);
			block.isLeafBlock = true;
			root = block;
			return;
		}
		
		/* Atleast one record is present in the block and
		now the current record needs to be inserted in the correct position*/
		else if (block != null || !block.key.isEmpty()) {
			
			for(int i=0;i<block.key.size();i++){
				
				String keyPresent = block.key.get(i);
				
				if(key.compareTo(keyPresent)<0){
					
					if(block.isLeafBlock){
						
						block.key.add(i, key);
						block.offsetval.add(i, offset);
						if(block.key.size()==MaxBlockSize){
							split(block);
						}
						return;
					}
					
					//current block is an intermediate block and key to be inserted is lesser than this so,
					//traverse to the left sub-block
					else{
						insertRecord(block.blockPtr.get(i), key, offset);
						return;
					}
				}
				
				else if(key.compareTo(keyPresent)>0){
					
					//Key to be inserted is greater than the key at this position. 
					//So continue searching for the correct position.
					if (i < block.key.size() - 1) {
						continue;
					}
					
					else{
						if (block.isLeafBlock){
							block.key.add(key);
							block.offsetval.add(offset);
							//split the block if the size of the block is exhausted
							if(block.key.size()==MaxBlockSize){
								split(block);
								return;
							}
							return;
						}
						
						//current block is an intermediate block and key to be inserted is greater than this so,
						//traverse to the left sub-block
						else{
							insertRecord(block.blockPtr.get(i + 1),key, offset);
							return;
						}
					}
				}
				
				else{
					System.out.println("Record already exists!");
					return;
				}
			}
			
		}
	}
	
	/**
	 * Split the block once it reaches the maxBlockSize
	 * @param block : block object of BPlusTree type to insert the record
	 * */
	public static void split(BPlusTree block){
		
		BPlusTree leftBlock = new BPlusTree();
		BPlusTree rightBlock = new BPlusTree();
		BPlusTree tempBlock = new BPlusTree();
		
		if(block.isLeafBlock){
			
			for(int i = 0;i<=splitIndex;i++){
				leftBlock.key.add(block.key.get(i));
				leftBlock.offsetval.add(block.offsetval.get(i));
			}
			
			for(int i=splitIndex+1;i<block.key.size();i++){
				rightBlock.key.add(block.key.get(i));
				rightBlock.offsetval.add(block.offsetval.get(i));
			}
			
			leftBlock.isLeafBlock = true;
			rightBlock.isLeafBlock = true;
			
			leftBlock.rightBlockPtr = rightBlock;
			leftBlock.leftBlockPtr = block.leftBlockPtr;
			
			rightBlock.leftBlockPtr = leftBlock;
			rightBlock.rightBlockPtr = block.rightBlockPtr;
			
			if(block.parent==null){
				tempBlock.blockPtr.add(leftBlock);
				tempBlock.blockPtr.add(rightBlock);
				tempBlock.key.add(rightBlock.key.get(0));
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;
				
				root = tempBlock;
				block = tempBlock;
			}
			
			else{
				tempBlock = block.parent;
				String splitKey = rightBlock.key.get(0);
				
				for(int i = 0;i<tempBlock.key.size();i++){
					if(splitKey.compareTo(tempBlock.key.get(i))<=0){
						tempBlock.key.add(i, splitKey);
						tempBlock.blockPtr.add(i, leftBlock);
						tempBlock.blockPtr.set(i+1, rightBlock);
						break;
					}
					else{
						if (i < tempBlock.key.size() - 1) {
							continue;
						}
						else{
							tempBlock.key.add(splitKey);
							tempBlock.blockPtr.add(i+1, leftBlock);
							tempBlock.blockPtr.set(i+2, rightBlock);
							break;
						}
					}
				}
				
				if (block.leftBlockPtr != null) {
					block.leftBlockPtr.rightBlockPtr = leftBlock;
					leftBlock.leftBlockPtr = block.leftBlockPtr;
				}
				if (block.rightBlockPtr != null) {
					block.rightBlockPtr.leftBlockPtr = rightBlock;
					rightBlock.rightBlockPtr = block.rightBlockPtr;
				}
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;
				
				//split the parent block also if the size of the block is exhausted				
				if (tempBlock.key.size() == MaxBlockSize) {
					split(tempBlock);
					return;
				}
				return;
			}
		}//end of leafBlock
		
		else{
			
			String splitKey = block.key.get(splitIndex);
			int i = 0,k=0;
			for(i=0;i<=splitIndex;i++){
				leftBlock.key.add(block.key.get(i));
				leftBlock.blockPtr.add(block.blockPtr.get(i));
				leftBlock.blockPtr.get(i).parent = leftBlock;
			}
			leftBlock.blockPtr.add(block.blockPtr.get(i+1));
			leftBlock.blockPtr.get(i+1).parent = leftBlock;
			
			for(i=splitIndex+2;i<block.key.size();i++){
				rightBlock.key.add(block.key.get(i));
				rightBlock.blockPtr.add(block.blockPtr.get(i));
				rightBlock.blockPtr.get(k++).parent = rightBlock;
			}
			rightBlock.blockPtr.add(block.blockPtr.get(i+1));
			rightBlock.blockPtr.get(k++).parent = rightBlock;
			
			if(block.parent==null){
				tempBlock.blockPtr.add(leftBlock);
				tempBlock.blockPtr.add(rightBlock);
				tempBlock.key.add(splitKey);
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;
				
				root = tempBlock;
				block = tempBlock;
			}
			
			else{
				tempBlock = block.parent;
				
				for(i = 0;i<tempBlock.key.size();i++){
					if(splitKey.compareTo(tempBlock.key.get(i))<=0){
						tempBlock.key.add(i, splitKey);
						tempBlock.blockPtr.add(i, leftBlock);
						tempBlock.blockPtr.set(i+1, rightBlock);
						break;
					}
					else{
						if (i < tempBlock.key.size() - 1) {
							continue;
						}
						
						else{
							tempBlock.key.add(splitKey);
							tempBlock.blockPtr.add(i+1, leftBlock);
							tempBlock.blockPtr.set(i+2, rightBlock);
							break;
						}
					}
				}
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;

				//split the parent block also if the size of the block is exhausted				
				if (tempBlock.key.size() == MaxBlockSize) {
					split(tempBlock);
					return;
				}
			
				return;
			}
		}
		
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
        
        
        int keyLength = 24;
        long offset = constants.TOTAL_SIZE - 24;// to calculate the offset of the records in the text input file
		MaxBlockSize = (1024 - keyLength) / keyLength + 8 ; // assuming each block of 1024 bytes and offset of long data type, hence 8 bytes
		splitIndex = (MaxBlockSize%2==0)?(MaxBlockSize/2)-1 : MaxBlockSize/2;
        
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
                        
                        
            			
                        String key = new String(sdtnameBytes); //extract key upto the specified keyLength from each line to insert in the index file
            			insertRecord(root,key, constants.TOTAL_SIZE - 24); // insert each new key offset pair of the record read from input file to the index file
            			//offset += line.length() + 2; //add 2 to each offset after adding line.length() for "\n" i.e. newline charaters
            			offset += constants.COUNTS_OFFSET + 4;
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

                        

                        
                    
                }
                
                
             
            }
            writefile( datafile, outputFileName);

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
