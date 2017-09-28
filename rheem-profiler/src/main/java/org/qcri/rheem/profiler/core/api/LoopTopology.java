package org.qcri.rheem.profiler.core.api;

import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

import java.util.Collection;
import java.util.Collections;
import java.util.Stack;

/**
 * Loop Topology implementation
 *
 */
public class LoopTopology extends TopologyBase implements Topology {

    public static final int INITIAL_INPUT_INDEX = 0;
    public static final int ITERATION_INPUT_INDEX = 1;

    public static final int ITERATION_OUTPUT_INDEX = 0;
    public static final int FINAL_OUTPUT_INDEX = 1;
    /**
     * Pipeline Topology that will be contained inside the Loop Topology
     */
    private PipelineTopology pipelineTopology;
    /**
     * Number of iterations
     */
    private Integer numIterations = -1;

    private static Stack<Topology> loops = new Stack<>();

    private static Stack<Topology> loopsCopies = new Stack<>();




    // 3 inputs ()

    // 2 outputs

    public LoopTopology(int topologyNumber){
        this.inputTopologySlots = new InputTopologySlot[2];
        this.outputTopologySlots = new OutputTopologySlot[2];
        this.inputTopologySlots[INITIAL_INPUT_INDEX] = new InputTopologySlot("initIn", this);
        this.inputTopologySlots[ITERATION_INPUT_INDEX] = new InputTopologySlot("iterIn", this);

        this.outputTopologySlots[ITERATION_OUTPUT_INDEX] = new OutputTopologySlot("iterOut", this);
        this.outputTopologySlots[FINAL_OUTPUT_INDEX] = new OutputTopologySlot("finOut", this);

        this.topologyNumber=topologyNumber;
    }

    public LoopTopology(int topologyNumber, int numIterations){
        this.inputTopologySlots = new InputTopologySlot[2];
        this.outputTopologySlots = new OutputTopologySlot[2];
        this.inputTopologySlots[INITIAL_INPUT_INDEX] = new InputTopologySlot("initIn", this);
        this.inputTopologySlots[ITERATION_INPUT_INDEX] = new InputTopologySlot("iterIn", this);

        this.outputTopologySlots[ITERATION_OUTPUT_INDEX] = new OutputTopologySlot("iterOut", this);
        this.outputTopologySlots[FINAL_OUTPUT_INDEX] = new OutputTopologySlot("finOut", this);

        this.numIterations = numIterations;
        this.topologyNumber=topologyNumber;
    }

    public void initialize(Topology initTopology, int initOpOutputIndex) {
        initTopology.connectTo(initOpOutputIndex, this, INITIAL_INPUT_INDEX);
    }

    public void beginIteration(Topology beginTopology, int beginInputIndex) {
        this.connectTo(ITERATION_OUTPUT_INDEX, beginTopology, beginInputIndex);
    }

    public void endIteration(Topology endTopology, int endOpOutputIndex) {
        endTopology.connectTo(endOpOutputIndex, this, ITERATION_INPUT_INDEX);
    }

    public void outputConnectTo(Topology outputTopology, int thatInputIndex) {
        this.connectTo(FINAL_OUTPUT_INDEX, outputTopology, thatInputIndex);
    }

    /**
     * create a copy of current topology
     * @return
     */
    public Topology createCopy(int topologyNumber){
        if (!loops.isEmpty())
            if (loops.peek()==this){
                // remove the top node
                loops.pop();
                // connect the out2
                // exit the loop
                return loopsCopies.pop();
            }



        LoopTopology newTopology = new LoopTopology(topologyNumber);


        // push the current loop
        loops.push(this);

        // push the loop copy
        loopsCopies.push(newTopology);
        // Clone the input topologies
        InputTopologySlot[] tmpInputTopologySlots = new InputTopologySlot[2];
        OutputTopologySlot[] tmpOutTopologySlots = new OutputTopologySlot[2];

        Integer counter=0;

        // Clone input slots
        for(InputTopologySlot in:this.inputTopologySlots){
            tmpInputTopologySlots[counter]=in.clone();

            if ((this.inputTopologySlots[counter].getOccupant() != null)&&(counter==1)){
                // case of cloning iteration input (iteration last node ) should be treated in a non recursive way to prevent infinity looping
                // input1 topology copy
                Topology tmpNewTopology = in.getOccupant().getOwner().createCopy(topologyNumber-1);
                // connect the input1Copy topology with the new junctureCopy input1
                tmpInputTopologySlots[counter].setOccupant(tmpNewTopology.getOutput(0));
            } else if ((this.inputTopologySlots[counter].getOccupant() != null)){
                    // input1 topology copy
                    Topology tmpNewTopology = in.getOccupant().getOwner().createCopy(topologyNumber-1);
                    // connect the input1Copy topology with the new junctureCopy input1
                    tmpInputTopologySlots[counter].setOccupant(tmpNewTopology.getOutput(0));
            }
            counter++;
        }

        // Reinitialize the counter
        /*counter=0;
        for(OutputTopologySlot out:this.outputTopologySlots){
            tmpOutTopologySlots[counter] = new OutputTopologySlot("in", newTopology);
            tmpOutTopologySlots[counter].setOccupiedSlots(out.getOccupiedSlots());

            // Increment output slot counter
            counter++;
        }*/

        // Prepare the output slots : with adding  finOut output slot
        OutputTopologySlot tmpOutTopologySlot = new OutputTopologySlot("finOut", newTopology);
        tmpOutTopologySlot.setOccupiedSlots(this.outputTopologySlots[1].getOccupiedSlots());

        // Add tmpInputTopologySlots
        newTopology.setInputTopologySlots(tmpInputTopologySlots);
        newTopology.setOutputTopologySlot(tmpOutTopologySlot,1);

        // Clone the nodes
        newTopology.setNodes((Stack) this.getNodes().clone());

        //Clone the nodenumber
        newTopology.setNodeNumber(this.nodeNumber);
        newTopology.setName(this.getName());
        return newTopology;
    }

    public Topology getLoopBodyOutput() {
        return this.getInput(ITERATION_INPUT_INDEX).getOccupant().getOwner();
    }

    public Topology getFinalLoopOutput() {
        return this.getOutput(FINAL_OUTPUT_INDEX).getOccupiedSlots().get(0).getOwner();
    }

    public Topology getLoopBodyInput() {
        if (!this.getOutput(ITERATION_OUTPUT_INDEX).getOccupiedSlots().isEmpty())
            return this.getOutput(ITERATION_OUTPUT_INDEX).getOccupiedSlots().get(0).getOwner();
        else
            return null;
    }

    public Topology getLoopInitializationInput() {
        return this.getInput(INITIAL_INPUT_INDEX).getOccupant().getOwner();
    }


}
