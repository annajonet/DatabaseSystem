package dubstep;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Iterator;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;

class TableIterator implements Iterator<PrimitiveValue[]>{

	
	BufferedReader tbr;
	FileInputStream ftr;
	int file_cur_pos = 0;
//	int file_prev_pos = 0;
	String record;
	QTable tab;
	String[] colval;
	PrimitiveValue[] cols;
	boolean checked_next=false;
	
	public TableIterator(QTable t) throws FileNotFoundException {
		// TODO Auto-generated constructor stub
		ftr = new FileInputStream(t.tab_loc);
		tbr = new BufferedReader(new InputStreamReader(ftr));
		tab = t;
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			if(checked_next) return true;
			file_cur_pos = (int) ftr.getChannel().position();
			if((record=tbr.readLine())!=null){
				checked_next = true;
				return true;
			}
			else return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public PrimitiveValue[] next() {
		// TODO Auto-generated method stub
		
		if(!checked_next){
			try {
				if((record=tbr.readLine())==null)
					return null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		checked_next = false;
		
		colval = new String[tab.n_attr];
		cols = new PrimitiveValue[tab.n_attr];
		
		if(record.length()<tab.n_attr) return null;

		int from_idx=0, cur_idx=0, incrt =0;
		while ((cur_idx = record.indexOf('|', from_idx)) != -1 && incrt<tab.n_attr) {
				colval[incrt] = record.substring(from_idx, cur_idx);
				cols[incrt] = getColVal(tab.col_nm[incrt]);
				incrt++;
				from_idx = cur_idx+1;
		}
		if(incrt<tab.n_attr){
			colval[incrt] = record.substring(from_idx);
		
			cols[incrt] = getColVal(incrt);
		}
		incrt++;

		return cols;
	}
	
	public int getRecLength(){
		return record.length();
	}

	public PrimitiveValue getColVal(Column c){
		int index = tab.t_mapper.get(c.getColumnName());

		switch(tab.col_type[index]){
		case 3:
			if(colval[index]==null) return null;
			return new StringValue(colval[index]);
		case 1:
			if(colval[index]==null) return null;
			return new DoubleValue(colval[index]);
		case 0:
			if(colval[index]==null) return null;
			return new LongValue(colval[index]);
		case 2:
			if(colval[index]==null) return null;
			return new DateValue(colval[index]);
		default:
			return null;
		}
	}
	public PrimitiveValue getColVal(int index){

		switch(tab.col_type[index]){
		case 3:
			if(colval[index]==null) return null;
			return new StringValue(colval[index]);
		case 1:
			if(colval[index]==null) return null;
			return new DoubleValue(colval[index]);
		case 0:
			if(colval[index]==null) return null;
			return new LongValue(colval[index]);
		case 2:
			if(colval[index]==null) return null;
			return new DateValue(colval[index]);
		default:
			return null;
		}
	}
	public PrimitiveValue getColVal(String col_name){
		int index = tab.t_mapper.get(col_name);

		switch(tab.col_type[index]){
		case 3:
			if(colval[index]==null) return null;
			return new StringValue(colval[index]);
		case 1:
			if(colval[index]==null) return null;
			return new DoubleValue(colval[index]);
		case 0:
			if(colval[index]==null) return null;
			return new LongValue(colval[index]);
		case 2:
			if(colval[index]==null) return null;
			return new DateValue(colval[index]);
		default:
			return null;
		}
	}
	
	public PrimitiveValue getPrimVal(){
		String Prim_val="";

		for(int i = 0; i<tab.prim_nm.size(); i++){
			int index = tab.t_mapper.get(tab.prim_nm.get(i));
			Prim_val += cols[index];
		}
		return new StringValue(Prim_val);
	}

	
}

class TabexperIterator implements Iterator<PrimitiveValue[]>{

	TableIterator tab_it;
	PrimitiveValue[] cols;
	ExprEval e;
	Expression expr;
	boolean hasRead;
	
	public TabexperIterator(QTable tab, Expression exp) throws FileNotFoundException {
		// TODO Auto-generated constructor stub
		
		tab_it = new TableIterator(tab);
		e = new ExprEval(tab_it);
		expr = exp;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(hasRead){
			return true;
		}
		while(tab_it.hasNext()){
			cols = tab_it.next();
			try {
//				System.out.println("Display columns: "+Arrays.toString(cols)+"-->"+e.eval(expr).toBool());
				if(e.eval(expr).toBool()){
					hasRead = true;
					return true;
//					break;
				}
			} catch (InvalidPrimitive e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return false;
	}

	@Override
	public PrimitiveValue[] next() {
		// TODO Auto-generated method stub
		if(hasRead){
			hasRead = false;
			return cols;
		}
		else
			return null;
	}
	
	public PrimitiveValue getColVal(String col_name){
		return tab_it.getColVal(col_name);
	}
	
	public PrimitiveValue getPrimVal(){
		
		return tab_it.getPrimVal();
		
	}

	
}