
import mcgui.*;

import java.util.*;


/**
 * Simple example of how to use the Multicaster interface.
 *
 * @author Andreas Larsson &lt;larandr@chalmers.se&gt;
 */
public class ExampleCaster extends Multicaster {

	int Rg;
	int Sg;
	int sequencer_id; // sequencer's id
	HashMap<String, ExtendMessage> HoldBackQueue; // the hold-back queue to store the messages that we have received but not delivered
	HashMap<String, ExtendMessage> ReceivedOrders; // store all the orders which have received
	//HashSet<String> ReceivedMessages; // store all the messages which have received
	//HashSet<String> ReceivedOrders; // store all the orders which have received
	// hold_back_queue
	final List<String> hold_back_queue = new LinkedList<String>();
	//UUID uuid = UUID.randomUUID();
	
    /**
     * No initializations needed for this simple one
     */
    public void init() {
        
        Rg = 0;
        Sg = 0;
        sequencer_id = hosts -1;
        //ReceivedMessages = new HashSet<String>();
        HoldBackQueue = new HashMap<String, ExtendMessage>();
        ReceivedOrders = new HashMap<String, ExtendMessage>();
        
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
            /* Sends to everyone except itself */
            if(i != id) {
                bcom.basicsend(i,message);
            }
        }
    }
    // Generates Sequencer Number and multicasts to other nodes
    private void generateSeqMulticast(int Sg){
    	// Increment Sequqncer Number by 1
    	Sg = Sg + 1;
    	// Create unique id for order
    	String order_id = createUniqueId();
    	ExtendMessage order = new ExtendMessage(id, "Order", order_id,ExtendMessage.TYPE_SEQ_ORDER);
        // Multicast order to other nodes
    	multicast(order);
    }
    
        
    /**
     * The GUI calls this module to multicast a message
     */
    // For process to R-multicast message to group
    public void cast(String messagetext) { /* messagetext is the input from UI */

    	String message_id = createUniqueId();
    	ExtendMessage message = new ExtendMessage(id, messagetext, message_id,ExtendMessage.TYPE_MESSAGE);
        multicast(message);
    	
        mcui.debug("Sent out: \""+messagetext+"\"");
        
        if(isSequencer()){
        	mcui.debug("I'm the sequencer.");
        	//generateSeqMulticast(Sg);
        	//mcui.deliver(id, messagetext, "from sequencer!");
        }
        else{
        	HoldBackQueue.put(message.getIdNumber(),message);
        }

        //mcui.deliver(id, messagetext, "from myself!");
    }
    
    
    private boolean isReliableMulticast(ExtendMessage received_message){
    	/* if the received message is the type of MESSAGE */
    	if(received_message.getType() == ExtendMessage.TYPE_MESSAGE){
			// the message received has not been put in HoldBackQueue
    		if(HoldBackQueue.containsKey(received_message.getIdNumber()) == false){
        		// if the node is not the sender
        		if(id != received_message.getSender()){
        			// R-multicast the received message to other nodes for reliability
        			multicast(received_message);
        		}
        	}
        	// the message received has already been put in HoldBackQueue
        	else{
        		mcui.debug("the MESSAGE has already been put in HoldBackQueue");
        		return true;
        	}
    	}
    	/* if the received message is the type of TYPE_SEQ_ORDER */
    	if(received_message.getType() == ExtendMessage.TYPE_SEQ_ORDER){
    		mcui.debug("TYPE_SEQ_ORDER");
    		// the order received has not been put in ReceivedOrders
    		if(ReceivedOrders.containsKey(received_message.getIdNumber()) == false){
        		// if the node is not the sender
        		if(id != received_message.getSender()){
        			// R-multicast the received message to other nodes for reliability
        			multicast(received_message);
        		}
        	}
        	// the message received has already been put in ReceivedOrders
        	else{
        		mcui.debug("the ORDER has already been put in ReceivedOrders");
        		return true;
        	}
    	}
    	
    	return false;
    	
    }
    
    /**
     * Receive a basic message
     * @param message  The message received
     */
    public void basicreceive(int peer,Message message) {
    	
    	ExtendMessage received_message = (ExtendMessage) message;
    	
    	if(isReliableMulticast(received_message)){ // if message has been received, then return
    		return;
    	}
    	
    	if(received_message.getType() == ExtendMessage.TYPE_MESSAGE){
    		// put the current received order in HoldBackQueue
			HoldBackQueue.put(received_message.getIdNumber(),received_message);
    	}
    	if(received_message.getType() == ExtendMessage.TYPE_SEQ_ORDER){
    		// put the current received order in ReceivedOrders
			ReceivedOrders.put(received_message.getIdNumber(),received_message);
    	}
    	
    	


    }

    /**
     * Signals that a peer is down and has been down for a while to
     * allow for messages taking different paths from this peer to
     * arrive.
     * @param peer	The dead peer
     */
    public void basicpeerdown(int peer) {
        mcui.debug("Peer "+peer+" has been dead for a while now!");
    }
    
    
 
    
    
}


class ExtendMessage extends Message {
    
    String text;
    String id_number;
    int type;
    static final int TYPE_SEQ_ORDER = 1;
    static final int TYPE_MESSAGE = 0;
        
    public ExtendMessage(int sender,String text,String id,int type) {
        super(sender);
        this.text = text;
        this.id_number = id;
        this.type = type;
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
    
    public static final long serialVersionUID = 0;
}
