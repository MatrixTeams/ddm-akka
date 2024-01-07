package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> WorkerLargeMessage;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		boolean result;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new HashMap<>();

		this.allData = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++){
			this.allData.add(new ArrayList<>());
		}
		this.finishedReading = false;
		this.readFileCounter = 0;
		this.taskKeys = new ArrayList<>();
		this.tasksResults = new HashMap<int[], Boolean>();
		this.idleWorkers = new ArrayList<>();
		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));

	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final Map<ActorRef<DependencyWorker.Message>, ActorRef<LargeMessageProxy.Message>> dependencyWorkers;

	private final List<List<Set<String>>> allData;

	private final List<int[]> taskKeys;
	private final List<ActorRef<DependencyWorker.Message>> idleWorkers;
	private final Map<ActorRef<DependencyWorker.Message>, int[]> busyWorkers = new HashMap<>();

	private boolean finishedReading;

	private int readFileCounter;
	private Map<int[], Boolean> tasksResults;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		//initialize a list of all columns of all tables.
		for (int id = 0; id < this.headerLines[message.getId()].length; id++){
			this.allData.get(message.getId()).add(new HashSet<>());
		}
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.

		//Store batches to allData list

		List<String[]> batch = message.getBatch();
		int tableId = message.getId();
		int numofcolumn =this.headerLines[tableId].length;
		for (String[] record : batch) {
			for (int i = 0; i < numofcolumn; i++) {
				this.allData.get(tableId).get(i).add(record[i]);

			}
		}

		//
		if (message.getBatch().size() != 0) {
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}else{
			this.readFileCounter++;

			if (readFileCounter == inputFiles.length)
				this.finishedReading = true;
		}
		// if allData list is filled, then start generating tasks
		if (finishedReading) {
			setTaskKeys();
			generateTasks();
		}

		return this;
	}

	//creating indexes for each column in tables, so we can use these indexes for passing the columns to the worker.
	private void setTaskKeys(){
		List<int[]> keys = new ArrayList<>();
		for (int table = 0; table < this.inputFiles.length; table++){
			for (int col = 0; col < this.headerLines[table].length; col++){
				int[] pair = {table, col};
				keys.add(pair);
			}
		}
		for(int dep = 0; dep < keys.size(); dep++){
			for(int ref = 0; ref < keys.size(); ref++){
				if (dep == ref)
					continue;

				int[] depArray = keys.get(dep);
				int[] refArray = keys.get(ref);
				int[] element = new int[]{depArray[0], depArray[1], refArray[0], refArray[1]};
				this.taskKeys.add(element);
			}
		}
		for (int[] task: this.taskKeys){
			this.tasksResults.put(task, null);
		}
	}
	private void generateTasks(){
		while(true){
			if(this.idleWorkers.isEmpty()){
				break;
			}
			//if we have an idle worker we assign task to it.
			assignTaskToWorker(this.idleWorkers.remove(0));
		}
	}
	private void assignTaskToWorker(ActorRef<DependencyWorker.Message> dependencyWorker){
		if (this.taskKeys.isEmpty()){
			this.idleWorkers.add(dependencyWorker);
		}else {

			// Prepare task columns
			//with the task keys that we have, we are accessing the columns (dependent column - reference column)
			//and storing it to a list (TaskColumns)

			int[] task = this.taskKeys.remove(0);

			List<Set<String>> taskColumns = new ArrayList<>();
			taskColumns.add(allData.get(task[0]).get(task[1])); // depColumn
			taskColumns.add(allData.get(task[2]).get(task[3])); // refColumn

			//this step is just for track and display the tables and columns keys
			List<Integer> taskTables = new ArrayList<>();
			taskTables.add(task[0]); // depTable key
			taskTables.add(task[1]); // depColumn key
			taskTables.add(task[2]); // refTable key
			taskTables.add(task[3]); // refColumn key

			//Using LargeMessageProxy to transfer data to the DependencyWorkers
			LargeMessageProxy.LargeMessage taskMessage = new DependencyWorker.TaskMessage(this.getContext().getSelf(), taskColumns, taskTables);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskMessage, this.dependencyWorkers.get(dependencyWorker)));
			// add the worker and the task to the busy workers map
			this.busyWorkers.put(dependencyWorker, task);
		}
	}
	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.containsKey(dependencyWorker)) {
			this.dependencyWorkers.put(dependencyWorker, message.WorkerLargeMessage);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

			//the pattern is used inside this assignTaskToWorker function
			assignTaskToWorker(dependencyWorker);
		}
		return this;
	}
	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

		boolean result = message.result;
		int[] task = busyWorkers.get(dependencyWorker);
		if (result) {
			int dependent = task[0];
			int referenced = task[2];
			File dependentFile = this.inputFiles[dependent];
			File referencedFile = this.inputFiles[referenced];
			String[] dependentAttributes = {this.headerLines[task[0]][task[1]]};
			String[] referencedAttributes = {this.headerLines[task[2]][task[3]]};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);

			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}


		this.busyWorkers.put(dependencyWorker, null);
		this.tasksResults.put(task, result);

		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		assignTaskToWorker(dependencyWorker);

		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (!this.tasksResults.containsValue(null))
			this.end();

		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		this.getContext().getLog().info("Shutting down Dependency Miner!");
		return Behaviors.stopped();
	}
	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);

		if(this.busyWorkers.containsKey(dependencyWorker) && this.busyWorkers.get(dependencyWorker) != null){
			this.taskKeys.add(this.busyWorkers.get(dependencyWorker));
			this.busyWorkers.remove(dependencyWorker);
		}
		return this;
	}
}