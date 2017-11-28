package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Sorting Algorithm , both in-memory and on disk
 *
 */
public class Sort {
	public static final String FILE_PATH = "temp";
	public static final String FINAL_FILE_PATH = "fin";
	public static final String TEST_DATA_PATH = "C:\\DBFiles\\test";
	List<String> fileList = new ArrayList<String>();
	public boolean inmem = true;
	List<Limit> limitList = new ArrayList<Limit>();
	int fileCount;
	boolean[] ascending;
	List<PrimitiveValue[]> data;
	String[] colType;
	Long maxSize;
	int threshold;
	List<BufferedWriter> writerList;
	int[] colIndex;
	int itr = 0;
	String itr_val = "";
	BufferedReader finReader;
	int extra_cols = 0;
	
	public boolean hasNext() throws IOException{
		boolean ret_val = false;
		itr_val = "";
		
		if(inmem){
			if(itr < data.size()){
				PrimitiveValue[] outNext = data.get(itr);
				for(int i=0; i<outNext.length; i++){
					itr_val += outNext[i]+"|";
				}
				itr_val = itr_val.substring(0, itr_val.length()-1);
				for(int j=0;j<extra_cols;j++){
					itr_val = itr_val.substring(0,itr_val.lastIndexOf('|'));
				}
				itr++;
				return true;
			}	
			return false;
		}else{
			if(itr == 0){
				finReader = new BufferedReader(new InputStreamReader(new FileInputStream(FINAL_FILE_PATH)));
			}
			itr_val = finReader.readLine();
			ret_val = (itr_val == null)?false:true;
			for(int j=0;j<extra_cols;j++){
				itr_val = itr_val.substring(0,itr_val.lastIndexOf('|'));
			}
			itr++;
			if(!ret_val) finReader.close();
			return ret_val;
		}
		
	}
	public String next(){
		return itr_val;
	}
	
	public void getColType(){
		colType = new String[colIndex.length];
		for(int i=0;i<colIndex.length;i++){
			if(data.get(0)[colIndex[i]] != null){
				colType[i] = data.get(0)[colIndex[i]].getClass().getName();
				//System.out.println(colType);
			}
		}
		
		
	}

	public Sort(LinkedHashMap<String, Integer> mapper/*
									 * true if sort order is ascending false if
									 * descending
									 */, ArrayList<OrderByElement> order_by, int extra_cols, boolean isJoin) {
		this.colIndex = new int[order_by.size()];
		this.ascending = new boolean[order_by.size()];
		for(int j=0; j<order_by.size(); j++){						
			Column c = (Column)order_by.get(j).getExpression();
			this.ascending[j] = order_by.get(j).isAsc();
			if(isJoin){
				if(c.getTable().getName()==null)
					this.colIndex[j] = mapper.get(c.getColumnName());
				else{
					try{
						this.colIndex[j] = mapper.get(c.getTable().getName()+"_"+c.getColumnName());
					}
					catch(Exception e){
						System.out.println(c.getTable().getName()+"_"+c.getColumnName());
					}
				}
			}
			else{
				this.colIndex[j] = mapper.get(c.getColumnName());
			}
		}
		this.extra_cols = extra_cols;

		
		fileList = new ArrayList<String>();	//temporary file names
		limitList = new ArrayList<Limit>();	//
		writerList = new ArrayList<BufferedWriter>();
		fileCount = -1;
//		this.ascending = ascending;
		data=new ArrayList<PrimitiveValue[]>();
		this.maxSize = Runtime.getRuntime().freeMemory();
		this.threshold = 0;
//		this.colIndex = colIndex;
	}

	public void calculateThreshold(){
		threshold = (int)Math.round(0.25 * data.size());
	}
	public void addData(PrimitiveValue[] record/* record to be added */) {
		data.add(record);
		if(colType == null){
			getColType();
		}
		//System.out.println("adding"+record[0]);
		if (remainingSize() == 0) {
			//System.out.println("Inside If of add data");
			calculateThreshold();
			fileWriteandSort();			
			inmem=false;
		}
	}

	public int remainingSize() {
		Long currentSize = Runtime.getRuntime().freeMemory();
		if(currentSize <= maxSize){
			if(currentSize <= maxSize/2){
				return 0;
			}else{
				return 1;
			}
		}else{
			maxSize = currentSize;
			return 1;
		}
		
	}

	public void newFile() {
		++fileCount;
		String fileName = FILE_PATH + fileCount;
		try {
			FileWriter fw = new FileWriter(fileName);
			writerList.add(new BufferedWriter(fw));
			fileList.add(fileName);
			
			limitList.add(new Limit(fileCount,null));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int compareAllLimitColumn(int listIndex, int dataIndex){
		for(int i=0; i<colIndex.length; i++){
			if(limitList.get(listIndex).getValue() != null){			
				int returnVal = PrimitiveComparator.compareValue(limitList.get(listIndex).getValue()[i],data.get(dataIndex)[colIndex[i]]);
				if(ascending[i]){
					if(returnVal < 0){
						return -1;
					}else if(returnVal > 0){
						return 1;
					}
				}else{
					if(returnVal > 0){
						return -1;
					}else if(returnVal < 0){
						return 1;
					}
				}
			}			
		}
		return 0;
	}
	public int searchIndex(int index) {//comparator
		
		int startIndex = 0, endIndex = limitList.size() - 1;
		while (endIndex >= startIndex) {
			int middle = (startIndex + endIndex) / 2;
			//if (ascending) {
				if (compareAllLimitColumn(middle, index) <= 0) { //limitList.get(middle) <= val
					return middle;
				} else {
					startIndex = middle + 1;
					middle = (startIndex + endIndex) / 2;
				}
			
		}
		return -1;
	}

	public void fileWriteandSort() {
		int i, startPos = 0;
		int index = -1;
		Collections.sort(data, new PrimitiveComparator(this.colIndex, this.ascending));
		
		for (i = 0; (i < data.size()) && (!fileList.isEmpty()); ++i) {
			index = searchIndex(i);
			if (index != -1) {
				startPos = i;
				break;
			}
		}
		if (index == -1) {
			newFile();
			index = fileList.size() - 1;
			String writeString = "";
			int endIndex=getEndIndex(0);
			try {
			for (i = 0; i < endIndex; ++i) {
				int j=0;
				writeString = "";
				for(; j<data.get(i).length; j++){
					writeString += data.get(i)[j].toString()+"|";					
				}
				writeString = writeString.substring(0,writeString.length()-1)+"\n";
				writerList.get(limitList.get(index).getIndex()).write(writeString);
			}
				PrimitiveValue[] subArray = new PrimitiveValue[colIndex.length];
				for(int j=0;j<colIndex.length;j++){
					subArray[j] = data.get(endIndex-1)[colIndex[j]];
				}
				limitList.get(index).setValue(subArray);//-1
				data.subList(0, endIndex).clear();//-1
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			String writeString = "";
			int endIndex=getEndIndex(startPos);
			try {
			for (i = startPos; i < endIndex; ++i) {
				int j=0;
				writeString = "";
				for(; j<data.get(i).length; j++){
					writeString += data.get(i)[j].toString()+"|";					
				}
				writeString = writeString.substring(0,writeString.length()-1)+"\n";
				writerList.get(limitList.get(index).getIndex()).write(writeString);
			}
				PrimitiveValue[] subArray = new PrimitiveValue[colIndex.length];
				for(int j=0;j<colIndex.length;j++){
					subArray[j] = data.get(endIndex-1)[colIndex[j]];
				}
				limitList.get(index).setValue(subArray);//-1
				data.subList(startPos, endIndex).clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		rearrangeLimitList(index);
	}

	public int getEndIndex(int currPos) {
		int pos=((threshold + currPos) < data.size()) ? (threshold +currPos): (data.size());
		return pos;
	}

	public void rearrangeLimitList(int index) {//comparator
		FileListComparator comp = new FileListComparator();
		Limit temp = limitList.get(index);
		limitList.remove(index);
		int newIndex = Collections.binarySearch(limitList, temp, comp);
		if (newIndex < 0) {
			limitList.add((newIndex * -1) - 1, temp);
		} else {
			limitList.add(newIndex, temp);
		}
	}
	
	public void flushWrite(){
		if(data.size()!=0){
				Collections.sort(data, new PrimitiveComparator(colIndex, ascending));
				if(inmem){
					return;
				}
			int i,index;
			newFile();
			index = fileList.size() - 1;
			String writeString = "";
			int endIndex=data.size();
			try {
			for (i = 0; i < endIndex; ++i) {
				int j=0;
				writeString = "";
				for(; j<data.get(i).length; j++){
					writeString += data.get(i)[j].toString()+"|";					
				}
				writeString = writeString.substring(0,writeString.length()-1)+"\n";
				writerList.get(limitList.get(index).getIndex()).write(writeString);
			}
				PrimitiveValue[] subArray = new PrimitiveValue[colIndex.length];
				for(int j=0;j<colIndex.length;j++){
					subArray[j] = data.get(endIndex-1)[colIndex[j]];
				}
				limitList.get(index).setValue(subArray);//-1
				data.subList(0, endIndex).clear();//-1
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void sort() {
		FileWriter fw;
		try {
			flushWrite();
			closeWriters();
			if(inmem){
				return;
			}
			fw = new FileWriter(FINAL_FILE_PATH);

			BufferedWriter finWrite = new BufferedWriter(fw);
			List<BufferedReader> reader = new ArrayList<BufferedReader>();
			List<PrimitiveValue[]> values = new ArrayList<PrimitiveValue[]>();
			List<String> records = new ArrayList<String>();
			
			Class classType;
			Constructor[] con = new Constructor[colIndex.length];
			for(int j=0; j<colType.length; j++){
				classType = Class.forName(colType[j]);
				con[j] = classType.getConstructor(String.class);
			}	
					
			
			for (int i = 0; i < fileList.size(); ++i) {
				
				reader.add(new BufferedReader(new InputStreamReader(new FileInputStream(fileList.get(i)))));
				String str = reader.get(i).readLine();
				
				if (str != null) {
					//change here	
					records.add(i, str);
					PrimitiveValue[] prims = new PrimitiveValue[colIndex.length];
					String[] rec = str.split("\\|");
					for(int j=0; j<colIndex.length;j++){
						prims[j] = (PrimitiveValue) con[j].newInstance(rec[colIndex[j]]);
					}
					values.add(i, prims);
				} else {
					values.add(i, null);
					records.add(i, null);
				}

			}

			int index = 0;
			while (index != -1) {
				index = getMinIndex(values);
				if (index != -1) {
					//change here - write col by col
					finWrite.write(records.get(index) + "\n");
					String str = reader.get(index).readLine();
					
					if (str != null) {
						records.set(index, str);
						PrimitiveValue[] prims = new PrimitiveValue[colIndex.length];
						String[] rec = str.split("\\|");
						for(int j=0; j<colIndex.length;j++){
							prims[j] = (PrimitiveValue) con[j].newInstance(rec[colIndex[j]]);
						}
						
						values.set(index, prims);
					}
					else{
						values.set(index, null);
						records.set(index, null);
					}
				}

			}
			finWrite.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public void closeWriters() throws IOException {
		for (int i = 0; i < writerList.size(); ++i) {
			writerList.get(i).close();
		}
	}
	
	public int compareAllColumn(PrimitiveValue[] list1, PrimitiveValue[] list2){
		for(int i=0; i<colIndex.length; i++){
			int returnVal = PrimitiveComparator.compareValue(list1[i],list2[i]);
			if(ascending[i]){
				if(returnVal < 0){
					return -1;
				}else if(returnVal > 0){
					return 1;
				}
			}else{
				if(returnVal > 0){
					return -1;
				}else if(returnVal < 0){
					return 1;
				}
			}
						
		}
		return 0;
	}
	public int getMinIndex(List<PrimitiveValue[]> list) {
		int index = -1;
		PrimitiveValue[] minElem = null;
		for (int i = 0; i < list.size(); ++i) {
			if (minElem == null && list.get(i) != null) {
				minElem = list.get(i);
				index = i;
			} else if (minElem != null && list.get(i) != null) {
					if (compareAllColumn(list.get(i),minElem) < 0 ) {
						minElem = list.get(i);
						index = i;
					}
		
			}
		}

		return index;
	}
	
	public void generateTestData(int dataSize){
		try {
			Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(TEST_DATA_PATH)));
			for(int i=0;i<dataSize;++i)
			{
				int x = (int) (Math.random()*1000);
				out.append(x + "\n");
			}
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
class Limit{
	int index;
	PrimitiveValue[] value;
	
	public Limit(int index, PrimitiveValue[] value){
		this.index = index;
		this.value = value;
	}
	
	public int getIndex(){
		return this.index;
	}
	
	public void setIndex(int ind){
		this.index = ind;
	}
	
	public PrimitiveValue[] getValue(){
		return this.value;
	}
	
	public void setValue(PrimitiveValue[] val){
		this.value = val;
	}
}


