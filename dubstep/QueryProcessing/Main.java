package dubstep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashMap;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Main {

	public static int EOL = System.lineSeparator().length();
	public static int seq = 0;
	
	
	public static void main(String[] args) throws IOException, ParseException, SQLException {
		// TODO Auto-generated method stub
		
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		
		HashMap<String, QTable> table_mapper = new HashMap<>();

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
				QTable qt = new QTable(Cre_q);
				qt.buildQIndex();
				table_mapper.put(((CreateTable) query).getTable().getName(), qt);
			}
			else if(query instanceof Select){
				Select sel_q = (Select)query;
				QueryProcessor qp = new QueryProcessor(sel_q.getSelectBody(), table_mapper);
				qp.readQuery();
			}
		}
	}

}
