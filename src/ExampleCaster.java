import mcgui.*;

import java.util.*;
import java.util.Map.Entry;


/**
 * Simple example of how to use the Multicaster interface.
 *
 * @author Andreas Larsson &lt;larandr@chalmers.se&gt;
 */
public class ExampleCaster extends Multicaster {

	int Rg; // sequence number of a normal node
	int Sg; // sequence number of a sequencer
	int sequencer_id; // sequencer's id
	HashMap<String, ExtendMessage> HoldBackQueue; // the hold-back queue to store the messages that we have received but not delivered
	HashMap<String, ExtendMessage> ReceivedMessages; // stores all the messages which have received
	HashMap<String, ExtendMessage> ReceivedOrders; // stores all the orders which have received
	final List<String> hold_back_queue = new LinkedList<String>(); // hold_back_queue
	int ALIVE; // sate of a alive node
	int CRASHED; // state of a crashed node
	int[] nodesStatus; // store the status of nodes. 0 = ALIVE 1 = CRASHED
	int[] nodeClocks; // store the vector clocks
	
	
    /**
     * No initializations needed for this simple one
     */
    public void init() {
    	
        Rg = 0;
        Sg = 0;
        sequencer_id = hosts -1;
        HoldBackQueue = new LinkedHashMap<String, ExtendMessage>();
        ReceivedMessages = new LinkedHashMap<String, ExtendMessage>();
        ReceivedOrders = new LinkedHashMap<String, ExtendMessage>();
        ALIVE = 0;
        CRASHED = 1;
        nodesStatus = new int[hosts];
        nodeClocks=new int[hosts];
        // Initiate the vector clock
        for(int i=0;i<hosts;i++){
        	nodeClocks[i] = 0;
        }
        
        mcui.debug("The network has "+hosts+" hosts!");
        if(isSequencer()){
        	mcui.debug("I'm the sequencer.");
        }
        else{
        	mcui.debug("The sequencer is "+sequencer_id);
        }
        
    }
    
    // Check if the node is the current sequencer
    private boolean isSequencer() {
        if(id == sequencer_id){
        	return true;
        }
        else{
        	return false;
        }
    }
    
    // Create unique id for message
    private String createUniqueId(){
    	String unique_id = UUID.randomUUID().toString().replaceAll("-", "");
    	return unique_id;
    }
    
    // B-Multicast message to nodes
    private void multicast(ExtendMessage message){
    	for(int i=0; i < hosts; i++) {
    		/* Sends to everyone except crashed node */
    		if(nodesStatus[i] != CRASHED){
    			bcom.basicsend(i,message);
    		}
        }
    }
    
        
    /**
     * The GUI calls this module to multicast a message
     */
    // For process to R-multicast message to group
    public void cast(String messagetext) { /* messagetext is the input from UI */
    	String message_id = createUniqueId();
    	nodeClocks[id]++;
    	ExtendMessage message = new ExtendMessage(id, messagetext, message_id,ExtendMessage.TYPE_MESSAGE,createClockMessage());
    	multicast(message); 	
        mcui.debug("Sent out: \""+messagetext+"\"");
    }
    
    // Check if the message has been received already
    private boolean isReliableMulticast(ExtendMessage received_message){
    	/* if the received message is the type of MESSAGE */
    	if(received_message.getType() == ExtendMessage.TYPE_MESSAGE){
    		// the message received has already been put in HoldBackQueue
    		if(ReceivedMessages.containsKey(received_message.getIdNumber()) == true){
    			//mcui.debug("the MESSAGE has already been put in HoldBackQueue");
        		return true;
        	}
    		// the message received has not been put in HoldBackQueue
        	else{
        		// R-multicast the received message to all nodes for reliability
    			multicast(received_message);
        		return false;
        	}
    	}
    	/* if the received message is the type of TYPE_SEQ_ORDER */
    	if(received_message.getType() == ExtendMessage.TYPE_SEQ_ORDER){
    		// the message received has already been put in ReceivedOrders
    		if(ReceivedOrders.containsKey(received_message.getIdNumber()) == true){
        		return true;
        	}
    		// the order received has not been put in ReceivedOrders
        	else{
        		// R-multicast the received message to all nodes for reliability
    			multicast(received_message);
        	}
    	}
    	return false;
    }
    
    // Create node's ClockMessage for transferring 
    private String createClockMessage(){
    	String Clocks="";
    	for(int i =0;i<hosts;i++){
    		if(i==0){
    			Clocks = Clocks + nodeClocks[i];
    		}
    		else{
    			Clocks = Clocks + "#" + nodeClocks[i];
    		}
    	}
    	return Clocks;
    }
    
    // Parse ClockMessage and return node's clock value
    private int parseClockMessage(ExtendMessage received_message,int node_id){
    	return Integer.parseInt(received_message.getClocks().split("#")[node_id]);
    }
    
    // Check whether the sequencer has received any message that the message sender had delivered at the time it multicast the message
    private boolean checkReceivedAnyMessageFromSender(ExtendMessage received_message){
    	for(int i=0;i<hosts;i++){
    		if(i!= received_message.getSender()){
    			// if the sequencer hasn't receive any message that the message sender had delivered at the time it multicast the message
    			if(parseClockMessage(received_message,i) > nodeClocks[i]){
        			return false;
        		}
    		}
    	}
    	return true;
    }
    
    // Create order message and multicast it
    private void createOrderMulticast(ExtendMessage received_message){
    	ExtendMessage order = new ExtendMessage(received_message.getSender(), received_message.getText()+"/"+String.valueOf(Sg), received_message.getIdNumber(),ExtendMessage.TYPE_SEQ_ORDER,createClockMessage());
    	// Multicast order to other nodes
    	multicast(order);
	    // Increment Sequqncer Number by 1
	    Sg = Sg + 1;
    }
    
    // Generates Sequencer Number and multicasts to other nodes
    private void generateSeqMulticast(ExtendMessage received_message){
    	// get message sender's id	
    	int sender_id = received_message.getSender();
    	while(true){
    		// if the multicast message is not from the sequencer then the sequencer decide whether to multicast order
    		if(sender_id != sequencer_id){
    			// if the sequencer has received all the previous messages from the sender
	    		if(parseClockMessage(received_message,sender_id) == nodeClocks[sender_id] +1 && checkReceivedAnyMessageFromSender(received_message) == true){
	    			// Increment vector clock by 1
	    			nodeClocks[sender_id]++;
	    			// Create order and multicast it to all nodes
	    			createOrderMulticast(received_message);
	    	    	break;
	    		}
	    	}
	    	// if the multicast message is from the sequencer then the sequencer multicast order to all nodes
	    	else{
	    		// Create order and multicast it to all nodes
		    	createOrderMulticast(received_message);
		   	    break;
	   		} 		
    	}
    }
    
    // Deliver message
    private void deliverMessage(ExtendMessage received_message){
    	while(true){	
    		if(HoldBackQueue.containsKey(received_message.getIdNumber()) && Rg == Integer.valueOf(received_message.getText().split("/")[1])){
    	        // Remove the message from HoldBackQueue
    			HoldBackQueue.remove(received_message.getIdNumber());
        		// get message sender's id	
    			int sender_id = received_message.getSender();
    			// if the receiver is not sequencer or the message sender then increments its vector clock by 1 according to the message sender
    			if( id != sender_id && isSequencer() == false ){
    				nodeClocks[sender_id]++;
    			}
        		
    			mcui.deliver(received_message.getSender(), received_message.getText().split("/")[0]);
        		Rg = Integer.valueOf(received_message.getText().split("/")[1]) + 1;

        		break;
    		}
    	}
    }
    
    /**
     * Receive a basic message
     * @param message  The message received
     */
    public void basicreceive(int peer,Message message) {
    	
    	ExtendMessage received_message = (ExtendMessage) message;

    	// if message has been received, then return
    	if(isReliableMulticast(received_message) == true){ 
    		return;
    	}
    	// If the message is text message
    	if(received_message.getType() == ExtendMessage.TYPE_MESSAGE){
    		// put the current received message in ReceivedMessages
    		ReceivedMessages.put(received_message.getIdNumber(),received_message);
    		// put the current received message in HoldBackQueue
			HoldBackQueue.put(received_message.getIdNumber(),received_message);
			if(isSequencer()){
				generateSeqMulticast(received_message);
			}
    	}
    	// If the message if order message
    	if(received_message.getType() == ExtendMessage.TYPE_SEQ_ORDER){
    		// put the current received order in ReceivedOrders
			ReceivedOrders.put(received_message.getIdNumber(),received_message);
			deliverMessage(received_message);
    	}

    }

    // Set new sequencer if the previous sequencer crashes
    private void setNewSequencer(){
    	for (int i = hosts - 1; i >= 0; i--){
    		if (nodesStatus[i] == 0) {
    			sequencer_id = i;
                break;
            }
    	}
    	if(isSequencer()){
        	mcui.debug("I'm the new sequencer.");
        	// The new sequencer to handle the crashing problem of the previous sequencer
        	actAsNewSequencer();
        }
        else{
        	mcui.debug("The new sequencer is "+sequencer_id);
        }
    }
    
    // The new sequencer need to handle the message that are not delivered because of the crash of the previous sequencer
    private void actAsNewSequencer(){
    	// find the last logic clock of mine when the previous sequencer dies
    	int minimal = 0;
    	for(Entry<String, ExtendMessage> entry:HoldBackQueue.entrySet()){
    		int temp = parseClockMessage(entry.getValue(),id);
    		if(minimal == 0){
    			minimal = temp;
    		}
    		if(minimal > temp){
    			minimal = temp;
    		}
		}
    	// Set the new sequencer's logic clock back to the previous moment when the previous crashed
    	int last_logic_clock;
    	if(minimal != 0){
    		last_logic_clock = minimal - 1;
    	}
    	else{
    		last_logic_clock = minimal;
    	}
    	nodeClocks[id] = last_logic_clock;
    	// Set new sequencer's Sg equals to its Rg
    	Sg = Rg;
    	// Generate Sequencer and Multicast
    	for(Entry<String, ExtendMessage> entry:HoldBackQueue.entrySet()){
    		// If the message for the HoldBackQueue is sent by this node
    		if(entry.getValue().getSender() == id){
    			nodeClocks[id]++;
    		}
    		// Generates Sequencer Number and multicasts to other nodes
    		generateSeqMulticast(entry.getValue());
    	}
    	
    }
    
    /**
     * Signals that a peer is down and has been down for a while to
     * allow for messages taking different paths from this peer to
     * arrive.
     * @param peer	The dead peer
     */
    public void basicpeerdown(int peer) {
        mcui.debug("Node "+peer+" crashes!");
        nodesStatus[peer] = CRASHED;
        // if the sequencer crashes, then set a new sequencer
        if(peer == sequencer_id){
        	setNewSequencer();
        }
    }
      
    
}


class ExtendMessage extends Message {
    
    String text;
    String id_number;
    int type;
    String Clocks;
    static final int TYPE_SEQ_ORDER = 1;
    static final int TYPE_MESSAGE = 0;
        
    public ExtendMessage(int sender,String text,String id,int type,String Clocks) {
        super(sender);
        this.text = text;
        this.id_number = id;
        this.type = type;
        this.Clocks = Clocks;
    }
    
    /**
     * Returns the text of the message only. The toString method can
     * be implemented to show additional things useful for debugging
     * purposes.
     */
    public String getText() {
        return text;
    }
    public String getIdNumber() {
        return id_number;
    }
    public int getType() {
        return type;
    }
    public String getClocks() {
        return Clocks;
    }
    
    public static final long serialVersionUID = 0;
}