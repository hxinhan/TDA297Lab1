/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.*;
import mcgui.*;


/**
 *
 * 
 */
public class CasterClass extends Multicaster {

    HashMap<Integer, String> Seq2msgIDMap;//Will hold the mapping between message unique Id to message sequence number in group
    HashMap<String, ExtendedMessage> bufferedMessages;//this will be a list of  Messages we have received but not delivered
    HashSet<String> allReceivedMessageIds;
    int currentGlobalSeqNumber;// the next global sequence number to be delivered
    int currentLocalMsgIdSeq;//local Id seq number used in coming up with a unique message Id
    int currentSequencer;
    int noOfSeqSentout;
    int[] nodesStatus;//will store the status of nodes. 0 = Alive 1 = Crashed
    int[] nodeClocks;

    @Override
    public void init() {
        Seq2msgIDMap = new HashMap<Integer, String>();
        bufferedMessages = new HashMap<String, ExtendedMessage>();
        allReceivedMessageIds = new HashSet<String>();
        currentSequencer = hosts - 1;//the one with the highest ID
        nodesStatus = new int[hosts];
        nodeClocks=new int[hosts];
        mcui.debug("Current group view has "+hosts+" hosts");
        if(amSequencer())mcui.debug("I am the Sequencer");
        else mcui.debug("The Sequencer is: "+currentSequencer);

    }

    private boolean amSequencer() {
        return id == currentSequencer;
    }

    private void multicast(ExtendedMessage message) {
        for (int i = 0; i < hosts; i++) {
            if (i != id && nodesStatus[i]==0) {
                bcom.basicsend(i, message);
            }
        }
    }

    private boolean runReliableReceive(int peer, ExtendedMessage message) {
        if (allReceivedMessageIds.contains(message.getMessageId()) || message.getSender() == id) 
            return false;
        allReceivedMessageIds.add(message.getMessageId());
        multicast(message);
        return true;
    }

    private void createSeqAndDeliverMessage(ExtendedMessage message) {
       
        int seqId = getNextGlobalSequence();
        ExtendedMessage seqMessage = new ExtendedMessage(id, ExtendedMessage.MESSAGE_TYPE_SEQ_ORDER,id+""+(++noOfSeqSentout), seqId + ExtendedMessage.SEQ_MESSAGE_DELIMETER + message.getMessageId());
        multicast(seqMessage);
        mcui.deliver(message.getSender(), message.getText());
    }

    private int getNextGlobalSequence() {
        return currentGlobalSeqNumber++;
    }

    private void deliverAnyDueMessages() {
        while (Seq2msgIDMap.containsKey(currentGlobalSeqNumber) && bufferedMessages.containsKey(Seq2msgIDMap.get(currentGlobalSeqNumber))) {//deliver the message
            ExtendedMessage message = bufferedMessages.remove(Seq2msgIDMap.remove(currentGlobalSeqNumber));
            mcui.deliver(message.getSender(), message.getText());
            currentGlobalSeqNumber++;
            nodeClocks[message.getSender()]++;
        }
    }

    private void checkAndSetWhoSequencerIs() {
        for (int i = hosts - 1; i >= 0; i--) 
            if (nodesStatus[i] == 0) {
                currentSequencer = i;
                break;
            }
    }

    private void generateAnyPendingSeqNos() {
        for(int i=0;i<hosts;i++){
            while(bufferedMessages.containsKey(createMsgIdFrom(i,nodeClocks[i]+1))){
                ExtendedMessage msg=bufferedMessages.remove(createMsgIdFrom(i,nodeClocks[i]+1));
                createSeqAndDeliverMessage(msg);
                nodeClocks[i]++;
            }
        }
    }
    
    private String getNewMessageId() {
        return createMsgIdFrom(id,++currentLocalMsgIdSeq);
    }
    
    

    @Override
    public void cast(String messagetext) {
        String msgId = getNewMessageId();
        ExtendedMessage message = new ExtendedMessage(id, ExtendedMessage.MESSAGE_TYPE_TEXT, msgId, messagetext);
         allReceivedMessageIds.add(msgId);
        multicast(message);
        mcui.debug("Sent out: \"" + messagetext + "\"");
        if (amSequencer()) {
            createSeqAndDeliverMessage(message);
        }else{
            this.bufferedMessages.put(message.getMessageId(), message);//we hold the message and wait for the sequence number
        }
    }

    @Override
    public void basicreceive(int peer, Message message) {
        ExtendedMessage msg = (ExtendedMessage) message;
        if (!runReliableReceive(peer, msg)) 
            return;//if false then either we have received it already or we sent it hence we return
        
        switch (msg.getMessageType()) {
            case ExtendedMessage.MESSAGE_TYPE_TEXT:
                this.bufferedMessages.put(msg.getMessageId(), msg);//we queue the message
                if (amSequencer()) {
                    generateAnyPendingSeqNos();
                } else {
                    deliverAnyDueMessages();
                }
                break;
            case ExtendedMessage.MESSAGE_TYPE_SEQ_ORDER:
                String[] mapString = msg.getText().split(ExtendedMessage.SEQ_MESSAGE_DELIMETER);
                if (this.Seq2msgIDMap.containsKey(Integer.parseInt(mapString[0]))) {
                    return;
                }
                this.Seq2msgIDMap.put(Integer.parseInt(mapString[0]), mapString[1]);
                deliverAnyDueMessages();
                break;
        }
    }

    @Override
    public void basicpeerdown(int peer) {
        nodesStatus[peer]=1;
        mcui.debug("PEER "+peer+" CRASHED!");
        mcui.debug("Current group view has "+hosts+" hosts");
        checkAndSetWhoSequencerIs();
        if(amSequencer()){
            mcui.debug("I am the Sequencer");
            generateAnyPendingSeqNos();
        }else
            mcui.debug("The Sequencer is: "+currentSequencer);
            
    }

    private String createMsgIdFrom(int node,int SeqId) {
        return node+"S"+SeqId;
    }

    
}
class ExtendedMessage extends Message {

    static final int MESSAGE_TYPE_SEQ_ORDER = 1;
    static final int MESSAGE_TYPE_TEXT = 0;
    static final String SEQ_MESSAGE_DELIMETER = "###";
    private String msgText;
    private String msgId;
    private int messageType;

    public ExtendedMessage(int sender, int msgType, String Id, String text) {
        super(sender);
        this.msgText = text;
        this.msgId = Id;
        this.messageType = msgType;
    }

    public int getMessageType() {
        return messageType;
    }

    public String getMessageId() {
        return msgId;
    }

    public String getText() {
        return msgText;
    }
    public static final long serialVersionUID = 0;
}
