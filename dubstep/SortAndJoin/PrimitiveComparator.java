package dubstep;

import java.sql.SQLException;
import java.util.Comparator;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;

public class PrimitiveComparator implements Comparator<PrimitiveValue[]>{
	int[] sortIndex;	
	boolean[] ascending;
	public PrimitiveComparator(int[] index, boolean[] asc){
		sortIndex = index;
		ascending = asc;
	}
	@Override
    public int compare(PrimitiveValue[] entry1, PrimitiveValue[] entry2)  { 
					
		for(int i=0; i< sortIndex.length; i++){
			int returnVal =compareValue(entry1[sortIndex[i]], entry2[sortIndex[i]]);
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
	public static int compareValue(PrimitiveValue val1, PrimitiveValue val2){ // returns <0 if val1 less than val2
		try{
			Eval e = new Eval() {
				
				@Override
				public PrimitiveValue eval(Column arg0) throws SQLException {
					// TODO Auto-generated method stub
					return null;
				}
			};
			
			if(val1 instanceof StringValue){
				return val1.toString().compareTo(val2.toString());    		
	    		
	    	}else{
	    		
					if(val1.equals(val2)) {
						return 0;
					}else if(e.eval(new GreaterThan(val2,val1)).toBool()) {
						return -1;
					}else{
						return 1;
					}					
				
	    	}
		} catch (InvalidPrimitive e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return -1;
	}
	
}
