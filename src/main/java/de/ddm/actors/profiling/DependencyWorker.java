package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage  {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		//ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		//int task;
		ActorRef<DependencyMiner.Message> replyTo;
		List<Set<String>> task;
		List<Integer> taskTablesNum;
		//List<List<Set<String>>> task;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}
	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
//		this.getContext().getLog().info("Working!");
		// I should probably know how to solve this task, but for now I just pretend some work...
		this.getContext().getLog().info("Compare table " +message.getTaskTablesNum().get(0) +" Column " + message.getTaskTablesNum().get(1) +" to table "+message.getTaskTablesNum().get(2) + " Column " + message.getTaskTablesNum().get(3));
		List<String> depColumnList = new ArrayList<>(message.getTask().get(0));
		List<String> refColumnList = new ArrayList<>(message.getTask().get(1));
		//comparing two columns to find IND
		//The parallelStream() method allows the comparison to be performed concurrently on multiple threads, potentially improving the performance for large sets of columns.
		boolean isSubset = refColumnList.parallelStream().allMatch(depColumnList::contains);

		message.getReplyTo().tell(new DependencyMiner.CompletionMessage(this.getContext().getSelf(), isSubset));

		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		this.getContext().getLog().info("Shutting down Dependency Worker!");
		return Behaviors.stopped();
	}
}
