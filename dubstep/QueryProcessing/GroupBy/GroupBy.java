package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

class GroupBy{
	
	TreeMap<String, PrimitiveValue[]> inmem_GroupBy = new TreeMap<>();
	Iterator<String> inmem_keylist = null;

	LinkedHashMap<String, Integer> gb_mapper = new LinkedHashMap<>();
	ArrayList<Column> group_col;
	int order_col;
	
	PrimitiveValue[] agg_cols;
	int[] agg_expr_val;
	Expression[] fn_exp;
	PrimitiveValue inct = new LongValue(1);
	PrimitiveValue inctd = new DoubleValue(1);
	QueryProcessor queryp;

	public GroupBy(QueryProcessor qp) {
		queryp = qp;
		
		group_col = qp.group_cols;
		
		for(int i=0; i<qp.projections.size(); i++){
			SelectItem sel_item = qp.projections.get(i);
			if(sel_item instanceof SelectExpressionItem){
				SelectExpressionItem attr_exp = (SelectExpressionItem) sel_item;
				if(attr_exp.getExpression() instanceof Column){
					Column c = (Column) attr_exp.getExpression();
					if(qp.join_list!=null)
						gb_mapper.put(c.getTable().getName()+"_"+c.getColumnName(), i);
					else
						gb_mapper.put(c.getColumnName(), i);
				}
				if(attr_exp.getAlias() != null){
					gb_mapper.put(attr_exp.getAlias(), i);
				}
			}
			else if(sel_item instanceof AllColumns){
				gb_mapper = qp.Qp_mapper;
			}
			
		}
		
//		setOrderByCols(gb_mapper, qp.projections.size());
		if(qp.order_by != null){
			int curr_size = qp.projections.size();
			for(int j=0; j<qp.order_by.size(); j++){	
				Expression or_exp = qp.order_by.get(j).getExpression();
				Column c = (Column)or_exp;
				if(gb_mapper.get(c.getColumnName()) == null){
					if(qp.order_expr == null){
						qp.order_expr = new ArrayList<Expression>();
					}
					qp.order_expr.add(or_exp);
					gb_mapper.put(c.getColumnName(),curr_size++);	
				}		
			}
		}
		
		//Create aggregation_projection
		order_col = (qp.order_expr == null)?0:qp.order_expr.size();

		agg_cols = new PrimitiveValue[qp.n_queryargs+order_col+1];
		agg_expr_val = new int[qp.n_queryargs+order_col+1];
		fn_exp = new Expression[qp.n_queryargs+order_col+1];
		agg_expr_val[qp.n_queryargs+order_col] = 5;
		fn_exp[qp.n_queryargs+order_col] = inct;

		for(int i=0; i<qp.n_queryargs; i++){
			if(qp.prj_expr.get(i) instanceof Function){
				Function col_fn = (Function)qp.prj_expr.get(i);
				switch(qp.prj_expr.get(i).toString().toUpperCase().substring(0, 3)){
				case "SUM" : agg_expr_val[i] = 0; break;
				case "MIN" : agg_expr_val[i] = 1; break;
				case "MAX" : agg_expr_val[i] = 2; break;
				case "COU" : agg_expr_val[i] = 3; break;
				case "AVG" : agg_expr_val[i] = 4; break;
				default : agg_expr_val[i] = -2; break;
				}
				if(!col_fn.toString().toUpperCase().equals("COUNT(*)"))
					fn_exp[i] = col_fn.getParameters().getExpressions().get(0);
				else{
					fn_exp[i] = inct;
				}
			}
			else{
				fn_exp[i] = qp.prj_expr.get(i);
				agg_expr_val[i] = -1;
			}

		}
		for(int i=0;i<order_col;i++){
			fn_exp[i+qp.n_queryargs] = qp.order_expr.get(i);
			agg_expr_val[i+qp.n_queryargs] = -1;
		}
	}
	
	public String gb_getKey(PrimitiveValue[] cols){

		String gb_key = "1";
		if(group_col!=null){
			for(int i = 0; i<group_col.size(); i++){
				int index;
				if(queryp.join_list==null)
					index = queryp.Qp_mapper.get(group_col.get(i).getColumnName());
				else
					index = queryp.Qp_mapper.get(group_col.get(i).getTable().getName()+"_"+group_col.get(i).getColumnName());
 				gb_key += cols[index];
			}
		}
		return gb_key;
	}

	public void addData(PrimitiveValue[] cols) throws SQLException{
		
		String gb_key = gb_getKey(cols);

		if(!ContainsKey(gb_key)){
			agg_cols = new PrimitiveValue[queryp.n_queryargs+order_col+1];
		}
		else{
			agg_cols = get(gb_key);
		}

		for(int i=0; i<queryp.n_queryargs+order_col+1; i++){
			PrimitiveValue cur_val;
			cur_val = queryp.e.eval(fn_exp[i]);
			if(cur_val==null) break;
			if(agg_cols[i]==null){
				if(agg_expr_val[i] == 3) agg_cols[i] = inct;
				else if(agg_expr_val[i] == 5) agg_cols[i] = inctd;
				else agg_cols[i] = cur_val;
			}
			else{
				switch(agg_expr_val[i]){ 
				case 0:
					agg_cols[i] = queryp.e.eval(new Addition(agg_cols[i],cur_val));
					
					break;
				case 1:
					if(queryp.e.eval(new GreaterThan(agg_cols[i],cur_val)).toBool()){
						agg_cols[i] = cur_val;
					}
					break;
				case 2:
					if(queryp.e.eval(new GreaterThan(cur_val,agg_cols[i])).toBool()){
						agg_cols[i] = cur_val;
					}
					break;
				case 3:
					agg_cols[i] = queryp.e.eval(new Addition(agg_cols[i],  inct));
					break;
				case 4:
					agg_cols[i] = queryp.e.eval(new Multiplication(agg_cols[i], agg_cols[queryp.n_queryargs+order_col]));
					agg_cols[i] = queryp.e.eval(new Addition(agg_cols[i],cur_val));
					PrimitiveValue count = queryp.e.eval(new Addition(agg_cols[queryp.n_queryargs+order_col], inctd));
					agg_cols[i] = queryp.e.eval(new Division(agg_cols[i], count));
					break;
				case 5:
					agg_cols[i] = queryp.e.eval(new Addition(agg_cols[i],  inctd));
					break;
				case -1:
					agg_cols[i] = cur_val;
					break;
				case -2:
				default: throw new java.sql.SQLException("ERR:Functions not defined:"+queryp.req_query);
				}
			}
		}
		put(gb_key, agg_cols);
	}
	
	public void put(String key, PrimitiveValue[] cols){
		inmem_GroupBy.put(key, cols);
	}
	public PrimitiveValue[] get(String key){
		return inmem_GroupBy.get(key);
	}
	
	public void newIterator(){
		inmem_keylist = inmem_GroupBy.keySet().iterator();
	}
	
	public boolean gb_hasNext(){
		if(inmem_keylist==null){
			newIterator();
		}
		return inmem_keylist.hasNext();
	}
	public PrimitiveValue[] gb_Next(){
		return inmem_GroupBy.get(inmem_keylist.next());
	}
	
	public boolean ContainsKey(String key){
		return inmem_GroupBy.containsKey(key);
	}
	
}
