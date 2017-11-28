package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;



class QIndex{

	QTable tab;
	ArrayList<String> index_col;
	HashMap<PrimitiveValue, PrimitiveValue[]> PrimaryIndex = new HashMap<>(); //In-memory
	HashMap<String, HashMap<PrimitiveValue, ArrayList<PrimitiveValue>>> index_mapper = new HashMap<>();
	HashMap<PrimitiveValue, Integer> ExternalIndex = new HashMap<>(); // RandomAccessIndex
	HashMap<String, HashMap<PrimitiveValue, String>> ex_index_mapper = new HashMap<>();  // for index type 2 or more
	
	public QIndex(QTable t, ArrayList<String> idx_nm) throws IOException{
		tab = t;
		index_col = idx_nm;
		for(int i=0; i<idx_nm.size(); i++){
			if(tab.index_type<=1){ //for index type 0 and 1
				HashMap<PrimitiveValue, ArrayList<PrimitiveValue>> SecondaryIndex = new HashMap<>();
				index_mapper.put(idx_nm.get(i), SecondaryIndex);
			}
			else{ // for index type 2 or more
				HashMap<PrimitiveValue, String> SecondaryIndex = new HashMap<>();
				ex_index_mapper.put(idx_nm.get(i), SecondaryIndex);
			}
		}
		build_index();
	}
	
	public void build_index() throws IOException{
		TableIterator tab_it = new TableIterator(tab);
		int cur_pos = 0;
		int rec_len = 0;
		while(tab_it.hasNext()){
			PrimitiveValue[] cols = tab_it.next();
			PrimitiveValue prim_val = tab_it.getPrimVal();

			for(int i = 0; i<index_col.size(); i++){
				PrimitiveValue indx_val = tab_it.getColVal(index_col.get(i));
				
				
				if(indx_val==null) continue;

				if(tab.index_type<=1){
					HashMap<PrimitiveValue, ArrayList<PrimitiveValue>> SecondaryIndex = index_mapper.get(index_col.get(i));
					if(SecondaryIndex==null)
						SecondaryIndex = new HashMap<>();
						
					ArrayList<PrimitiveValue> s_prim = SecondaryIndex.get(indx_val);
					if(s_prim==null)
						s_prim = new ArrayList<>();
					s_prim.add(prim_val);
					SecondaryIndex.put(indx_val, s_prim);
					index_mapper.put(index_col.get(i), SecondaryIndex);
				}
				else{
					String idx_tab_loc = tab.tab_name+"_"+index_col.get(i)+"_"+indx_val+".csv";
					File file = new File(idx_tab_loc);
					
					HashMap<PrimitiveValue, String> Ext_Index = ex_index_mapper.get(index_col.get(i));
					if(Ext_Index==null) Ext_Index = new HashMap<>();
					
					if(!file.exists()){
						file.createNewFile();
						Ext_Index.put(indx_val, idx_tab_loc);
					}
					Ext_Index.put(indx_val, idx_tab_loc);
					BufferedWriter ibw = new BufferedWriter(new FileWriter(file.getAbsolutePath(),true));
					
					String recrd = "";
					for(int k=0; k<cols.length; k++){
						recrd += cols[k].toString().replaceAll("'", "")+"|";
					}
					
					ibw.write(recrd.substring(0, recrd.length()-1));
					ibw.newLine();
					
					ibw.close();
				}
			}
			if(tab.index_type==0)
				PrimaryIndex.put(prim_val, cols);
			else if(tab.index_type==1){
				rec_len = tab_it.record.length();
				ExternalIndex.put(prim_val, cur_pos);
				
				cur_pos += rec_len+Main.EOL;
			}
			
		}
	}
	
	
	public ArrayList<PrimitiveValue[]> getData(String Col_nm, PrimitiveValue Ser_key) throws IOException{
		
		HashMap<PrimitiveValue, ArrayList<PrimitiveValue>> SecondaryIndex = index_mapper.get(Col_nm);
		ArrayList<PrimitiveValue> prim_list = SecondaryIndex.get(Ser_key);
		ArrayList<PrimitiveValue[]> record_list = new ArrayList<>();

		if(prim_list==null){
			return record_list;
		}
		
			//In-mem Index
			if(tab.index_type==0){
				for(int i=0; i<prim_list.size(); i++){
					record_list.add(PrimaryIndex.get(prim_list.get(i)));
				}
			}
			//External Index 
			else if(tab.index_type==1){
//				RandomAccessFile ra = new RandomAccessFile(tab.tab_loc, "r");
				BufferedReader rbr = new BufferedReader(new InputStreamReader(new FileInputStream(tab.tab_loc)));
				int prev_pos = 0;
				for(int i=0; i<prim_list.size(); i++){
//					ra.seek(ExternalIndex.get(prim_list.get(i)));
//					System.out.println(prim_list.get(i)+"->"+ExternalIndex.get(prim_list.get(i)));
					try{
						rbr.skip(ExternalIndex.get(prim_list.get(i))-prev_pos);
					}
					catch(Exception e){
						if(ExternalIndex.containsKey(prim_list.get(i))){
							System.out.println("has key for "+prim_list.get(i));
						}
						else{
							System.out.println("Doesn't have key for "+prim_list.get(i));
						}
						if(prim_list.get(i)!=null){
							System.out.println("Last Prim-list val: "+prim_list.get(i));
						}
						System.out.println("Exception @ "+i+"/"+prim_list.size()+":"+(prev_pos));
					}
					String rec = rbr.readLine();
					String[] colval = new String[tab.n_attr];
					PrimitiveValue[] cols = new PrimitiveValue[tab.n_attr];
					int from_idx=0, cur_idx=0, incrt =0;
					while (((cur_idx = rec.indexOf('|', from_idx)) != -1) && incrt<tab.n_attr) {
							colval[incrt] = rec.substring(from_idx, cur_idx);
							incrt++;
							from_idx = cur_idx+1; 
					}
					if(incrt<tab.n_attr)
						colval[incrt] = rec.substring(from_idx);
					incrt++;
					
//					System.out.println("ColVal:"+Arrays.toString(colval));
					IOException exception = new IOException();
					
					for(int j = 0 ; j<tab.n_attr; j++){
						try{
						switch(tab.col_type[j]){
						case 3:
							if(colval[j]==null)  cols[j] = null;
							else cols[j] = new StringValue(colval[j]);
							break;
						case 1:
							if(colval[j]==null) cols[j] = null;
							else cols[j] = new DoubleValue(colval[j]);
							break;
						case 0:
							if(colval[j]==null) cols[j] = null;
							else cols[j] = new LongValue(colval[j]);
							break;
						case 2:
							if(colval[j]==null) cols[j] = null;
							else cols[j] = new DateValue(colval[j]);
							break;
						default:
							cols[j] = null;
							break;
						}
					}
						catch(Exception e){
							throw new IOException(" ---VALUE!!!!---  "+Arrays.toString(colval));
						}
					}
					
					record_list.add(cols);
					prev_pos = ExternalIndex.get(prim_list.get(i))+rec.length()+Main.EOL;
				}
			}
			else if(tab.index_type==2){
				HashMap<PrimitiveValue, String> Ext_Index = ex_index_mapper.get(Col_nm);
				String tab_loc = Ext_Index.get(Ser_key);
//				ArrayList<PrimitiveValue[]> record_list = new ArrayList<>();
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(tab_loc)));
			
				String recrd="";
				while((recrd=br.readLine())!=null){ // split line @ read line time
//					if(recrd.equals(nullrcd)) continue;
					String[] cols = new String[tab.n_attr];
					PrimitiveValue[] colval = new PrimitiveValue[tab.n_attr];
					int from_idx=0, cur_idx=0, incrt =0;
					while ((cur_idx = recrd.indexOf('|', from_idx)) != -1) {
							cols[incrt++] = recrd.substring(from_idx, cur_idx);
							from_idx = cur_idx+1;
						}
					cols[incrt++] = recrd.substring(from_idx);
					
					for(int i = 0 ; i<tab.n_attr; i++){
						switch(tab.col_type[i]){
						case 3:
							if(cols[i]==null)  colval[i] = null;
							colval[i] = new StringValue(cols[i]);
							break;
						case 1:
							if(colval[i]==null) colval[i] = null;
							colval[i] = new DoubleValue(cols[i]);
							break;
						case 0:
							if(colval[i]==null) colval[i] = null;
							colval[i] = new LongValue(cols[i]);
							break;
						case 2:
							if(colval[i]==null) colval[i] = null;
							colval[i] = new DateValue(cols[i]);
							break;
						default:
							colval[i] = null;
							break;
						}
					}
					record_list.add(colval);
				}
			}
		return record_list;
	}
}
