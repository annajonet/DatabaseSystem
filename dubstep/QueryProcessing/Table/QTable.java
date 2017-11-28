package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

/*
 * Int/Long = 0
 * Decimal/Double = 1
 * Date = 2
 * Varchar/char/string = 3
 */

/*
 * index_type = 
 * in-memory = 0
 * ex-RandomAccess = 1
 * ex-SplitByValue = 2
 * ex-SplitByCol(Sort) = 3
 */

public class QTable {

	
	CreateTable table_struct;
	LinkedHashMap<String, Integer> t_mapper = new LinkedHashMap<>();
	String tab_loc;
	String tab_name;
	int n_attr;
	int[] col_type;
	String[] col_nm;
	int index_type;
	
	ArrayList<String> indx_nm = new ArrayList<>();
	ArrayList<String> prim_nm = new ArrayList<>();
	QIndex s_index;
	
	public QTable(CreateTable table){
		table_struct = table;
		tab_name = table_struct.getTable().getName();
		tab_loc = "data/"+tab_name+".csv";
		
		File file = new File(tab_loc);
		if(file.length()/(1024*1024)<100){
			index_type = 0;
		}
		else{
			index_type = 0;
		}

		ArrayList<ColumnDefinition> attr_list = (ArrayList<ColumnDefinition>) table_struct.getColumnDefinitions();
		n_attr = attr_list.size();
		col_type = new int[n_attr];
		col_nm = new String[n_attr];
		for(int i=0; i<attr_list.size(); i++){
			t_mapper.put(attr_list.get(i).getColumnName(), i);
			col_nm[i] = attr_list.get(i).getColumnName();

			switch(attr_list.get(i).getColDataType().toString().toUpperCase().substring(0, 3)){
			case "STR":
			case "VAR":
			case "CHA": 
				col_type[i]=3; 
				break;
			case "DEC":
				col_type[i]=1;
				break;
			case "INT":
				col_type[i]=0;
				break;
			case "DAT":
				col_type[i]=2;
				break;
			default:
				col_type[i]=-1;
				break;
			}
		}
		
	}
	
	public void buildQIndex() throws IOException{
			s_index = new QIndex(this, indx_nm);
			
			//Print Index*****************************************************************
/*			BufferedWriter bw = new BufferedWriter(new FileWriter("Indexs.txt"));
			
			Iterator<String> idx_col = s_index.index_mapper.keySet().iterator();
			while(idx_col.hasNext()){
				String col_nm = idx_col.next();
				if(index_type==0)
					System.out.println("Index of "+col_nm);
				else{
					bw.write("Index of "+col_nm);
					bw.newLine();
				}
				HashMap<PrimitiveValue, ArrayList<PrimitiveValue>> SI = s_index.index_mapper.get(col_nm);
				Iterator<PrimitiveValue> val_it = SI.keySet().iterator();
				while(val_it.hasNext()){
					PrimitiveValue val = val_it.next();
					if(index_type==0)
						System.out.print("["+val+"]:->");
					else
						bw.write("["+val+"]:->");
					ArrayList<PrimitiveValue> pks = SI.get(val);
					for(int i=0; i<pks.size(); i++){
						if(index_type==0)
							System.out.print(pks.get(i)+"->");
						else
							bw.write(pks.get(i)+"->");
						if(index_type==0)
							System.out.print("("+s_index.PrimaryIndex.get(pks.get(i))[0]+")");
						else if(index_type==1)
							bw.write("("+s_index.ExternalIndex.get(pks.get(i))+")");
					}
					if(index_type==0)
						System.out.println("||");
					else{
						bw.write("||");
						bw.newLine();
					}
				}
			}
			bw.close();
			//Print index end************************************************************
*/	}
	
	public boolean hasIndex(String col_nm){
		
		if(s_index.index_mapper.get(col_nm)==null)
			return false;
		else
			return true;
	}
	
	public String getColType(Column c){
		int idx = t_mapper.get(c.getColumnName());
		switch(col_type[idx]){
		case 0: return "INT";
		case 1: return "DECIMAL";
		case 2: return "DATE";
		case 3: return "STRING";
		default: return null;
		}
	}
	public String getColType(String col_name){
		int idx = t_mapper.get(col_name);
		switch(col_type[idx]){
		case 0: return "INT";
		case 1: return "DECIMAL";
		case 2: return "DATE";
		case 3: return "STRING";
		default: return null;
		}
	}
	
	
	
	
}