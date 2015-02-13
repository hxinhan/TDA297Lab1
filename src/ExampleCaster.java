
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
	HashSet<String> ReceivedMessages; // store all the messages which have received
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
        ReceivedMessages = new HashSet<String>();
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
            	//messagetext = messagetext+':'+UUID.randomUUID();
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
    	ExtendMessage message = new ExtendMessage(id, messagetext, message_id,ExtendMessage.MESSAGE_TYPE_TEXT);
        multicast(message);
    	
        mcui.debug("Sent out: \""+messagetext+"\"");
        if(isSequencer()){
        	mcui.debug("I'm the sequencer.");
        }
        else{
        	mcui.debug("Hold to wait. Put the message in hold-back queue");
        	HoldBackQueue.put(message.getMessageId(),message);
        	
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
    	// the message received does not belong to ReceivedMessages
    	if(ReceivedMessages.contains(received_message.getMessageId()) == false){
    		// add the current received message to ReceivedMessages set
    		ReceivedMessages.add(received_message.getMessageId());
    		if(received_message.getSender() != id){
    			multicast(received_message);
    		}
    		mcui.deliver(received_message.getSender(), received_message.getText());
    	}
    	// the message received already belongs to ReceivedMessages
    	else{
    		return;
    	}
    	
    		
    	Sg = Sg+1;
    		
    	

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
    
    String message_text;
    String message_id;
    int message_type;
    static final int MESSAGE_TYPE_SEQ_ORDER = 1;
    static final int MESSAGE_TYPE_TEXT = 0;
        
    public ExtendMessage(int sender,String text,String id,int type) {
        super(sender);
        this.message_text = text;
        this.message_id = id;
        this.message_type = type;
    }
    
    /**
     * Returns the text of the message only. The toString method can
     * be implemented to show additional things useful for debugging
     * purposes.
     */
    public String getText() {
        return message_text;
    }
    public String getMessageId() {
        return message_id;
    }
    public int getMessageType() {
        return message_type;
    }
    
    public static final long serialVersionUID = 0;
}
