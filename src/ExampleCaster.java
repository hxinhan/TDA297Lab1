
import mcgui.*;

import java.util.UUID;
import java.util.LinkedList;
import java.util.List;
import java.io.*;


/**
 * Simple example of how to use the Multicaster interface.
 *
 * @author Andreas Larsson &lt;larandr@chalmers.se&gt;
 */
public class ExampleCaster extends Multicaster {

	int Rg = 0;
	int Sg = 0;
	/* assume that no.0 node is the sequencer */
	int sequencer_id = 0;
	// hold_back_queue
	final List<String> hold_back_queue = new LinkedList<String>();
	//UUID uuid = UUID.randomUUID();
	
	
    /**
     * No initializations needed for this simple one
     */
    public void init() {
        mcui.debug("The network has "+hosts+" hosts!");
    }
        
    /**
     * The GUI calls this module to multicast a message
     */
    public void cast(String messagetext) { /* messagetext is the input from UI */
        for(int i=0; i < hosts; i++) {
            /* Sends to everyone except itself */
            if(i != id) {
            	messagetext = messagetext+':'+UUID.randomUUID();
                bcom.basicsend(i,new ExampleMessage(id, messagetext));
            }
        }
        mcui.debug("Sent out: \""+messagetext+"\"");
        // Add message to the hold-back queue
        hold_back_queue.add(messagetext);
        //mcui.deliver(id, messagetext, "from myself!");
    }
    
    /**
     * Receive a basic message
     * @param message  The message received
     */
    public void basicreceive(int peer,Message message) {
    	// check if the current node is the sequencer
    	if(sequencer_id == getId()){
    		mcui.debug("I'm the SEQUENCER!");
    		String messagetext = ((ExampleMessage)message).text;
    		String[] unique_id = ((ExampleMessage)message).text.split(":");
    		System.out.println("--"+unique_id[0]);
    		System.out.println("--"+unique_id[1]);
    		System.out.println("--"+unique_id[2]);
    	}
    	else{
    		mcui.deliver(peer, ((ExampleMessage)message).text);
    	}
    	
        //mcui.deliver(peer, ((ExampleMessage)message).text);
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



