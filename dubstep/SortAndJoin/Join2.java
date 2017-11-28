package dubstep;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;

public class Join2 {

	ArrayList<QueryProcessor> joinRecords;	//same order as joinIndex
	List<ArrayList<Integer>> joinIndex;
	
	HashMap<String, ArrayList<PrimitiveValue[]>> data;
	ArrayList<PrimitiveValue[]> output = new ArrayList<PrimitiveValue[]>();
	boolean external = false;
	int nextIndex = 0;
	
	public Join2(List<ArrayList<Integer>> joinIndex){
		this.joinIndex = joinIndex;
		this.data = new HashMap<String, ArrayList<PrimitiveValue[]>>();
		
	}
	
	public boolean hasNext(){
		if(nextIndex < output.size()){
			return true;
		}
		return false;
	}
	
	public PrimitiveValue[] next(){
		return output.get(nextIndex++);
	}
	
	
	public void addData(ArrayList<QueryProcessor> records) throws InvalidPrimitive, SQLException{
		
		this.joinRecords = records;	
		addToMap();
		try {
			TestOutput();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addToMap() throws InvalidPrimitive, SQLException{
		
		data = new HashMap<String, ArrayList<PrimitiveValue[]>>();
		
		ArrayList<Integer> joinOn = joinIndex.get(0);
		
		while(joinRecords.get(0).hasReadRecord()){		
			
			PrimitiveValue[] record = joinRecords.get(0).readRecord();	
			String key = "";
			for(int index =0; index<joinOn.size(); index= index+2){
				key = (key.isEmpty())?record[joinOn.get(index)].toRawString():key + "|" + record[joinOn.get(index)].toRawString();
			}
			ArrayList<PrimitiveValue[]> hashList = data.get(key);
			if(hashList == null){
				hashList = new ArrayList<PrimitiveValue[]>();
			}
			hashList.add(record);
			data.put(key, hashList);
		}
		
		/*try {
			TestDataOut();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		int joinOnCount = 1;
		HashMap<String, ArrayList<PrimitiveValue[]>> combined = new HashMap<String, ArrayList<PrimitiveValue[]>>();
		
		for(int i = 1; i<joinRecords.size();i++){	
		//while(joinOnCount < joinIndex.size()){

			combined = new HashMap<String, ArrayList<PrimitiveValue[]>>();
			joinOn = joinIndex.get(joinOnCount-1);
			while(joinRecords.get(i).hasReadRecord()){	
				
				PrimitiveValue[] record = joinRecords.get(i).readRecord();	
				String key = "";
				for(int index =1; index<joinOn.size(); index= index+2){
					key = (key.isEmpty())?record[joinOn.get(index)].toRawString():key + "|" + record[joinOn.get(index)].toRawString();
				}
				ArrayList<PrimitiveValue[]> hashList = data.get(key);
				if(hashList != null){
					for(PrimitiveValue[] arr: hashList){
						PrimitiveValue[] both = new PrimitiveValue[arr.length+record.length];
						int k =0;
						for(PrimitiveValue prim: arr){
							both[k] = prim;
							k++;
						}
						for(PrimitiveValue prim: record){
							both[k] = prim;
							k++;
						}
						if(joinOnCount < joinIndex.size()){
							//still tables left to join							
							ArrayList<Integer> joinOn2 = joinIndex.get(joinOnCount);
							String key2 = "";
							for(int index =0; index<joinOn2.size(); index= index+2){
								key2 = (key2.isEmpty())?both[joinOn2.get(index)].toRawString():key2 + "|" + both[joinOn2.get(index)].toRawString();
							}
							ArrayList<PrimitiveValue[]> nextList = combined.get(key2);
							if(nextList == null){
								nextList = new ArrayList<PrimitiveValue[]>();
							}
							nextList.add(both);
							combined.put(key2, nextList);
						}else{
							//last output
							
							output.add(both);
						}
					}					
				}
			}
			joinOnCount++;
			data = combined;				
		}
		
	}
	public void TestDataOut() throws IOException{
		Iterator<String> keyItr = data.keySet().iterator();
		Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("test_out.txt")));
		while(keyItr.hasNext()){
			String key = keyItr.next();
			ArrayList<PrimitiveValue[]> vals = data.get(key);
			//System.out.println(key);
			out.append("\n==========================="+key+"===========================\n");
			for(int i=0; i<vals.size(); i++){
				PrimitiveValue[] prim_val = vals.get(i);
				for(int j=0;j<prim_val.length;j++){
					System.out.println(prim_val[j].toRawString()+",");
					out.append(prim_val[j].toRawString()+",");
				}
				out.append("\n");
			}
		}
		out.close();
	}
	public void TestOutput() throws IOException{
		Iterator<PrimitiveValue[]> keyItr = output.iterator();
		Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("final_out.txt")));
		while(keyItr.hasNext()){
			PrimitiveValue[] nextVal = keyItr.next();	
			for(int i=0; i<nextVal.length; i++){				
				out.append(nextVal[i].toRawString()+",");
			}			
			out.append("\n");
		}
		out.close();
	}
}
