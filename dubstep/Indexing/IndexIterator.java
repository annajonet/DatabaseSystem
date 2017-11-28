package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

class IndexIterator implements Iterator<PrimitiveValue[]>{
	
	QIndex index;
	QTable tab;
	PrimitiveValue ser_val;
	int rec_pos = 0;
	int records_size;
	int expr_type;
	String idx_col;
	ArrayList<PrimitiveValue[]> records = new ArrayList<>();
	PrimitiveValue[] cols;

	Eval ev = new Eval() {
		
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}
	};
	
	public IndexIterator(QIndex Indx, String Col_nm, PrimitiveValue val, int e_t) throws InvalidPrimitive, SQLException, IOException {
		// TODO Auto-generated constructor stub
		index = Indx;
		ser_val = val;
		tab = Indx.tab;
		expr_type = e_t;
		idx_col = Col_nm;
		records = getDataexp();
		records_size = records.size();
	}
	
	public ArrayList<PrimitiveValue[]> getDataexp() throws InvalidPrimitive, SQLException, IOException{
		ArrayList<PrimitiveValue[]> recrds = new ArrayList<>();
		HashMap<PrimitiveValue, ArrayList<PrimitiveValue>> SecondaryIndex = index.index_mapper.get(idx_col);
		Set colval = SecondaryIndex.keySet();
		Iterator<PrimitiveValue> colval_it = colval.iterator();
		switch(expr_type){
		case 0:
			recrds = index.getData(idx_col, ser_val);
			break;
		case 1:
			while(colval_it.hasNext()){
				PrimitiveValue key_val = colval_it.next();
				if(ev.eval(new NotEqualsTo(key_val, ser_val)).toBool()){
					recrds.addAll(index.getData(idx_col, key_val));
				}
			}
			break;
		case 2:
			while(colval_it.hasNext()){
				PrimitiveValue key_val = colval_it.next();
				if(ev.eval(new GreaterThan(key_val, ser_val)).toBool()){
					recrds.addAll(index.getData(idx_col, key_val));
				}
			}
			break;
		case 3:
			while(colval_it.hasNext()){
				PrimitiveValue key_val = colval_it.next();
				if(ev.eval(new GreaterThanEquals(key_val, ser_val)).toBool()){
					recrds.addAll(index.getData(idx_col, key_val));
				}
			}
			break;
		case 4:
			while(colval_it.hasNext()){
				PrimitiveValue key_val = colval_it.next();
				if(ev.eval(new MinorThan(key_val, ser_val)).toBool()){
					recrds.addAll(index.getData(idx_col, key_val));
				}
			}
			break;
		case 5:
			while(colval_it.hasNext()){
				PrimitiveValue key_val = colval_it.next();
				if(ev.eval(new MinorThanEquals(key_val, ser_val)).toBool()){
					recrds.addAll(index.getData(idx_col, key_val));
				}
			}
			break;
		}
//		System.out.println("Retrun Record size:"+recrds.size());
		return recrds;
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
//		System.out.println("Cam to idx_itr @ hasNext()"+rec_pos+"<"+records_size);
		if(rec_pos<records_size)
			return true;
		else
			return false;
	}

	@Override
	public PrimitiveValue[] next() {
		// TODO Auto-generated method stub
		cols = records.get(rec_pos);
		rec_pos++;
		return cols;
	}

	public PrimitiveValue getColVal(Column c){
		int index = tab.t_mapper.get(c.getColumnName());
		
		return cols[index];

	}
	public PrimitiveValue getColVal(String col_name){
		int index = tab.t_mapper.get(col_name);

		return cols[index];
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
