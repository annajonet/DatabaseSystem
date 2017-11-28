package dubstep;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

class joiner{
	String alias_name;   //ch
	HashMap<String, Expression> jn_exp = new HashMap<>();
	ArrayList<Expression> tb_exp = new ArrayList<>();
	boolean is_alias; //ch
	
	public joiner(String name, boolean flag) {
		// TODO Auto-generated constructor stub
		alias_name = name; //ch
		is_alias = flag; //ch
	}
}


public class ReadQuery {


	public static void queryAnalyzer(PlainSelect p_sel, int level){
		FromItem from = p_sel.getFromItem();
		ArrayList<Join> join_list =  new ArrayList<>();
		if(p_sel.getJoins() != null){
			Expression wh_exp = p_sel.getWhere();
			System.out.println("From :"+ from);
			System.out.println("From Alias:"+ from.getAlias());
			System.out.println("Joins :"+ p_sel.getJoins());
			join_list = (ArrayList<Join>) p_sel.getJoins();
			for(int i = 0; i< join_list.size(); i++){
				
				System.out.println(join_list.get(i).toString()+"Join conditions :"+ join_list.get(i).getUsingColumns());
			}
			
			System.out.println("Query @ "+level+":"+p_sel);

			join_list = (ArrayList<Join>) p_sel.getJoins();

			LinkedHashMap<String, joiner> join_def = new LinkedHashMap<>();
			
	//		joiner jn = new joiner(from.toString());
			String col_alias = null; //ch
			if(from.getAlias()!=null) //ch
				col_alias = from.getAlias(); //ch
			join_def.put(((Table) from).getName(), new joiner(col_alias, false)); // ch
			if(from.getAlias()!=null) //ch
				join_def.put(col_alias, new joiner(((Table) from).getName(), true)); //ch
			//ch
			//ch
			
			for(int i=0; i<join_list.size(); i++){
				Join jn_tab = join_list.get(i);
				//alias not added
				System.out.println("join-->"+jn_tab.toString());
				String join_nm = jn_tab.toString(); //ch
				String[] join_nms = join_nm.split(" AS "); //ch
				if(join_nms.length<2){ //ch
					join_def.put(join_nm, new joiner(null, false)); //ch
				} //ch
				else{ //ch
					join_def.put(join_nms[0], new joiner(join_nms[1], false)); //ch
					join_def.put(join_nms[1], new joiner(join_nms[0], true)); //ch
				}
			}
			
			ArrayList<Expression> exp_list = new ArrayList<>();
			Expression exprn = wh_exp;
			while(exprn != null){
				if(exprn instanceof AndExpression){ // OR not handled
					AndExpression exp = (AndExpression)exprn;
					exp_list.add(exp.getRightExpression());
					exprn = exp.getLeftExpression();
				}
				else{
					exp_list.add(exprn);
					exprn = null;
				}
			}
			System.out.println("Where Coditions:"+exp_list);

			Expression rem_where_exp = null; 
			for(int i=0; i<exp_list.size(); i++){
				Expression exp = exp_list.get(i);
				if(exp instanceof BinaryExpression){
					BinaryExpression bi_exp = (BinaryExpression)exp;
					Column lhs; Expression re;
					if(bi_exp.getLeftExpression() instanceof Column){
						lhs = (Column) bi_exp.getLeftExpression();
						if(lhs.getTable().getName()==null){
							System.out.println("**"+lhs.getColumnName());
						}
						else{
							System.out.println(lhs.getTable().getName()+"_"+lhs.getColumnName());
						}
						re = bi_exp.getRightExpression();
					}
					else{
						lhs = (Column) bi_exp.getRightExpression();
						re = bi_exp.getLeftExpression();
					}
					if(re instanceof Column){
						Column rhs = (Column)re;
						if(join_def.containsKey(lhs.getTable().getName())){
							joiner jd = join_def.get(lhs.getTable().getName());
							if(jd.is_alias){ //ch
								jd = join_def.get(jd.alias_name); //ch
							} //ch
							jd.jn_exp.put(rhs.getTable().getName(), exp);
						}
						if(join_def.containsKey(rhs.getTable().getName())){
							joiner jd = join_def.get(rhs.getTable().getName());
							if(jd.is_alias){ //ch
								jd = join_def.get(jd.alias_name); //ch
							} //ch
							jd.jn_exp.put(lhs.getTable().getName(), exp);
						}
					}
					else{
						if(join_def.containsKey(lhs.getTable().getName())){
							joiner jd = join_def.get(lhs.getTable().getName());
							if(jd.is_alias){ //ch
								jd = join_def.get(jd.alias_name); //ch
							} //ch
							jd.tb_exp.add(exp);
						}
						else{
							if(rem_where_exp==null){
								rem_where_exp = exp;
							}
							else{
								rem_where_exp = new AndExpression(rem_where_exp, exp);
							}
						}
					}
				}
				else{
					if(rem_where_exp==null){
						rem_where_exp = exp;
					}
					else{
						rem_where_exp = new AndExpression(rem_where_exp, exp);
					}
				}
				
			}

			Iterator<String> join_it1 = join_def.keySet().iterator();
			while(join_it1.hasNext()){
				String tab = join_it1.next();
				joiner jn = join_def.get(tab);
				System.out.println(tab+" --> "+jn.jn_exp+"--"+jn.tb_exp);
			}
			Iterator<String> join_it = join_def.keySet().iterator();
			while(join_it.hasNext()){
				String tab = join_it.next();
				joiner jn = join_def.get(tab);
				if(!jn.is_alias){ //ch
					ArrayList<Expression> table_wh_exp = jn.tb_exp;
					String j_sel = "SELECT * FROM "+tab;
					if(jn.alias_name!=null) //ch
						j_sel += " AS "+jn.alias_name;//ch
					if(table_wh_exp.size()>0)
						j_sel +=" WHERE ";
					for(int i=0; i<table_wh_exp.size(); i++){
						j_sel += table_wh_exp.get(i);
						if(i==table_wh_exp.size()-1){
							j_sel +=";";
						}
						else{
							j_sel += " AND ";
						}
					}
					System.out.println("Query "+j_sel);
				}
			}
		}
		else if(from instanceof Table){
			
			ArrayList<SelectItem> sel_item = (ArrayList<SelectItem>) p_sel.getSelectItems();
			for(int i=0; i<sel_item.size(); i++){
				System.out.println(sel_item.get(i)+" - "+ sel_item.get(i).getClass());
				SelectExpressionItem sel_exp_item = (SelectExpressionItem)sel_item.get(i);
				System.out.println(sel_exp_item.getExpression().getClass());
			}
			
		}
		else if(from instanceof SubSelect){

			List<SelectItem> sel_list = p_sel.getSelectItems();
			Expression wh_exp = p_sel.getWhere();
			ArrayList<OrderByElement> order_by = (ArrayList<OrderByElement>) p_sel.getOrderByElements();
			ArrayList<Column> group_cols = (ArrayList<Column>) p_sel.getGroupByColumnReferences();
			Limit limiter =p_sel.getLimit();
			String from_name = from.getAlias();
			
			String query_form = "SELECT "+sel_list.toString();
			if(from_name!=null && from_name.length()>0){
				query_form +=" FROM ("+from_name+")";
			}
			else{
				query_form +=" FROM (--)";
			}
			if(wh_exp!=null){
				query_form +="WHERE "+wh_exp.toString();
			}
			if(group_cols!=null && !group_cols.isEmpty()){
				query_form += " GROUP BY "+group_cols.toString();
			}
			if(order_by != null && !order_by.isEmpty()){
				query_form += " ORDER BY "+order_by.toString();
			}
			if(limiter!=null){
				query_form +=" LIMIT "+limiter.getRowCount();
			}
			System.out.println("Query @ "+level+":"+query_form);
			
			SubSelect sub_sel = (SubSelect)from;
			PlainSelect sel_sp = (PlainSelect) sub_sel.getSelectBody();
			queryAnalyzer(sel_sp, level+1);
		}
		else if(from instanceof SubJoin){
			SubJoin query_jn = (SubJoin)from;
			
			List<SelectItem> sel_list = p_sel.getSelectItems();
			Expression wh_exp = p_sel.getWhere();
			ArrayList<OrderByElement> order_by = (ArrayList<OrderByElement>) p_sel.getOrderByElements();
			ArrayList<Column> group_cols = (ArrayList<Column>) p_sel.getGroupByColumnReferences();
			Limit limiter =p_sel.getLimit();

			FromItem tab1 = query_jn.getLeft();
			Join jn_op = query_jn.getJoin();
			FromItem tab2 = jn_op.getRightItem();
			Expression jn_exp = jn_op.getOnExpression();
			
			String query_form = "SELECT "+sel_list.toString();
			query_form += tab1 +" joins with "+tab2+" on "+jn_exp;
			if(wh_exp!=null){
				query_form +="WHERE "+wh_exp.toString();
			}
			if(group_cols!=null && !group_cols.isEmpty()){
				query_form += " GROUP BY "+group_cols.toString();
			}
			if(order_by != null && !order_by.isEmpty()){
				query_form += " ORDER BY "+order_by.toString();
			}
			if(limiter!=null){
				query_form +=" LIMIT "+limiter.getRowCount();
			}
			System.out.println("Query @ "+level+": "+query_form);
			
			if(tab1 instanceof SubSelect){
				SubSelect sb_sel = (SubSelect)tab1;
				PlainSelect sel_p = (PlainSelect)sb_sel.getSelectBody();
				queryAnalyzer(sel_p, level+1);
			}
			if(tab2 instanceof SubSelect){
				SubSelect sb_sel = (SubSelect)tab2;
				PlainSelect sel_p = (PlainSelect)sb_sel.getSelectBody();
				queryAnalyzer(sel_p, level+1);
			}
		}
		
	}
	
	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		
		int level = 1;

		while(true){
			System.out.print("$> ");
			String reqQ = "";
			while(true){
				String query = in.readLine();
				int semi_pos = query.indexOf(";");
				if(semi_pos==-1)
					reqQ += query+" ";
				else{
					reqQ += query.substring(0, semi_pos);
					break;
				}
			}
			reqQ = reqQ.toUpperCase();
			CCJSqlParser parser = new CCJSqlParser(new StringReader(reqQ));
			Statement query = parser.Statement();
			// Create an object of create query table type
			if(query instanceof CreateTable){ 
				CreateTable Cre_q = (CreateTable) query;
				System.out.println("Create Statement");
			}
			else if(query instanceof Select){
				Select sel_q = (Select)query;
				PlainSelect p_sel = (PlainSelect) sel_q.getSelectBody();
				queryAnalyzer(p_sel, level);
			}
		}

	}

}
