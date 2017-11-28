package dubstep;

import java.util.ArrayList;

class Node{

	static final int key_size = 4;
	int n_child;
	Elem[] child_nodes = new Elem[key_size];
	Node next;
	
	Node(int nc){
		n_child = nc;
		next = null;
	}
	
	public int getNodeSize(){
		return key_size;
	}
}
class Elem{
		Comparable key; 
		Object val;
		Node Next;
		
		public Elem(Comparable k, Node nxt){
			key = k;
			val = null;
			Next = nxt;
		}

		public Elem(Comparable k, Object v){
			key = k;
			val = v;
			Next = null;
		}
		
	}

public class BPTree<Key extends Comparable<Key>, Value> {
	
	
	Node root;
	int height;
	int pair_count;
	int key_size;
	Node start_node=null;
	
	public BPTree() {
		// TODO Auto-generated constructor stub
		root =  new Node(0);
		key_size = root.getNodeSize();
	}
	
	public int getHeight(){
		return height;
	}
	
	public int size(){
		return pair_count;
	}
	
	public boolean isEmplty(){
		if(pair_count==0){
			return true;
		}
		else{
			return false;
		}
	}
	
	public Value get(Key key){
		return search(root, key, height);
	}
	
	public ArrayList<Value> getRange(Key startkey, Key endkey){
		ArrayList<Value> data  = new ArrayList<>();
		Node st_node;
		int st_pnt=0;
		if(startkey == null) 
			st_node = start_node;
		else{
			st_node = searchNode(root, startkey, height);
			Elem[] leafNodes = st_node.child_nodes;
			for(int i=0; i<st_node.n_child; i++){
				if(eq(startkey, leafNodes[i].key)){
					st_pnt = i;
					break;
				}
			}
		}
		if(endkey == null){
			while(st_node!=null){
				Elem[] dataset = st_node.child_nodes;
				for(int i=st_pnt; i<st_node.n_child; i++){
					data.add((Value) dataset[i].val);
				}
				st_pnt = 0;
				st_node=st_node.next;
//				if(st_node==null) break;
			}
		}
		else{
			Elem cur_data = st_node.child_nodes[st_pnt];
			while(less(cur_data.key, endkey) || eq(cur_data.key, endkey)){
				data.add((Value) cur_data.val);
				
				st_pnt++;
				if(st_node.n_child==st_pnt){
					st_node = st_node.next;
					st_pnt=0;
				}
				if(st_node!=null){
					cur_data = st_node.child_nodes[st_pnt];
				}
				else{
					break;
				}
			}
		}
		
		return data;
	}
	
	public Value search(Node r, Key k, int ht){
		
		
		if(ht!=0){
			Elem[] keys = r.child_nodes;
			for(int i=0; i<r.n_child; i++){
				if(i+1==r.n_child||less(k,keys[i+1].key))
					return search(keys[i].Next, k, ht-1);
			}
		}
		else{
			Elem[] leafNodes = r.child_nodes;
			for(int i=0; i<r.n_child; i++){
				if(eq(k, leafNodes[i].key)){
					return (Value) leafNodes[i].val;
				}
			}
			
		}
		return null;
	}
	
	public Node searchNode(Node r, Key k, int ht){
		
		
		if(ht!=0){
			Elem[] keys = r.child_nodes;
			for(int i=0; i<r.n_child; i++){
				if(i+1==r.n_child||less(k,keys[i+1].key))
					return searchNode(keys[i].Next, k, ht-1);
			}
		}
		else{
			Elem[] leafNodes = r.child_nodes;
			for(int i=0; i<r.n_child; i++){
				if(eq(k, leafNodes[i].key)){
					return r;
				}
			}
			
		}
		return null;
	}
	
	public void put(Key key, Value val){
		
		Node splt_node = insert(root, key, val, height);
		pair_count++;
		
		if(splt_node==null) return;
		
		Node new_node = new Node(2);
		
		//need to split root
		new_node.child_nodes[0] = new Elem(root.child_nodes[0].key, root);
		new_node.child_nodes[1] = new Elem(splt_node.child_nodes[0].key, splt_node);
		root = new_node;
		height++;
	}
	
	private Node insert(Node h, Key key, Value val, int ht){
        Elem t = new Elem(key, val);

        int i;
        // internal node
        if (ht != 0) {
            for (i = 0; i < h.n_child; i++) {
                if ((i+1 == h.n_child) || less(key, h.child_nodes[i+1].key)) {
                    Node u = insert(h.child_nodes[i++].Next, key, val, ht-1);
                    if (u == null) return null;
                    t.key = u.child_nodes[0].key;
                    t.Next = u;
                    break;
                }
            }
        }

        // external node
        else {
            for (i = 0; i < h.n_child; i++) {
                if (less(key, h.child_nodes[i].key)) break;
            }
        }

        for (int j = h.n_child; j > i; j--)
            h.child_nodes[j] = h.child_nodes[j-1];
        if(start_node==null)
        	start_node = h;
        h.child_nodes[i] = t;
        h.n_child++;
        if (h.n_child < key_size) return null;
        else         return split(h);
    }

	private Node split(Node h) {
        Node t = new Node(key_size/2);
        h.n_child = key_size/2;
        for (int j = 0; j < key_size/2; j++)
            t.child_nodes[j] = h.child_nodes[key_size/2+j]; 
        h.next = t;
        return t;    
    }
	
    public String toString() {
        return toString(root, height, "") + "\n";
    }

    private String toString(Node h, int ht, String indent) {
        StringBuilder s = new StringBuilder();
        Elem[] children = h.child_nodes;

        if (ht == 0) {
            for (int j = 0; j < h.n_child; j++) {
                s.append(indent + children[j].key + " " + children[j].val + "\n");
            }
        }
        else {
            for (int j = 0; j < h.n_child; j++) {
                if (j > 0) s.append(indent + "(" + children[j].key + ")\n");
                s.append(toString(children[j].Next, ht-1, indent + "     "));
            }
        }
        return s.toString();
    }
    
    private boolean less(Comparable k1, Comparable k2) {
        return k1.compareTo(k2) < 0;
    }

    private boolean eq(Comparable k1, Comparable k2) {
        return k1.compareTo(k2) == 0;
    }

}

