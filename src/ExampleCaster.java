
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
	//HashSet<String> ReceivedMessages; // store all the messages which have received
	HashSet<String> ReceivedOrders; // store all the orders which have received
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
        }
        else{
        	mcui.debug("Hold to wait. Put the message in hold-back queue");
        	HoldBackQueue.put(message.getIdNumber(),message);
        }
        
        // Add message to the hold-back queue
        //hold_back_queue.add(messagetext);
        mcui.deliver(id, messagetext, "from myself!");
    }
    
    /**
     * Receive a basic message
     * @param message  The message received
     */
    public void basicreceive(int peer,Message message) {
    	
    	ExtendMessage received_message = (ExtendMessage) message;
    	
    	/* if the received message is the type of MESSAGE */
    	if(received_message.getType() == ExtendMessage.TYPE_MESSAGE){
    		// the message received does not belong to ReceivedMessages
    		/*
        	if(ReceivedMessages.contains(received_message.getIdNumber()) == false){
        		// add the current received message to ReceivedMessages set
        		ReceivedMessages.add(received_message.getIdNumber());
        		// if the node is not the sender
        		if(id != received_message.getSender()){
        			// R-multicast the received message to other nodes for reliability
        			multicast(received_message);
        			// the sender has already put the message in the hold-back queue when it sends this message
        			HoldBackQueue.put(received_message.getIdNumber(),received_message);
        		}
        		//HoldBackQueue.put(received_message.getIdNumber(),received_message);
        	}
        	*/
    		// the message received has not been put in HoldBackQueue
    		if(HoldBackQueue.containsKey(received_message.getIdNumber()) == false){
    			mcui.debug("the message received has not been put in HoldBackQueue");
        		// add the current received message to ReceivedMessages set
    			HoldBackQueue.put(received_message.getIdNumber(),received_message);
        		// if the node is not the sender
        		if(id != received_message.getSender()){
        			// R-multicast the received message to other nodes for reliability
        			mcui.debug("the node is not the sender");
        			multicast(received_message);
        		}
        	}
        	// the message received has already been put in HoldBackQueue
        	else{
        		mcui.debug("the message received has already been put in HoldBackQueue");
        		return;
        	}
    	}
    	
    	/* if the received message is the type of TYPE_SEQ_ORDER */
    	if(received_message.getType() == ExtendMessage.TYPE_SEQ_ORDER){
    		mcui.debug("TYPE_SEQ_ORDER");
    	}
    	
    		
    	//Sg = Sg+1;
    		
    	

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
