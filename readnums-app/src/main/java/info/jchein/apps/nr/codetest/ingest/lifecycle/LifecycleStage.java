package info.jchein.apps.nr.codetest.ingest.lifecycle;


public enum LifecycleStage
{
   /**
    * No segment object ever exists in this state.  It is a pseudo-state used by anything that needs to track segment state
    * to account for duration before a segment's constructor is invoked.
    */
   UNDER_CONSTRUCTION,
   
   /**
    * State presented by a Segment object immediately on return from its constructor.  Indicates component is ready to start.
    */
   READY,
   
   /**
    * The segment is in its primary worker state.
    */
   ACTIVE,
   
   /**
	 * An attempt to initialize the segment failed. It was READY, but did not become ACTIVE.
	 */
	FAILED_TO_START,

	/**
	 * The segment has suffered a disabling exceptional condition. The fault may or may not be recoverable. Resources are
	 * not necessarily released, and may not even be capable of aborting. Segments at FAULT may given the benefit of the
	 * doubt during a shutdown until all other segments have reached a quiescent state. AT that point, end user is given
	 * an option to manually accelerate shutdown. If no response is given, at timeout expiration the process stops--there
	 * is no transition to NOT_RESPONDING.
	 */
   //FAULT,
   
   /**
    * The segment has been signaled to release resources for a graceful shutdown
    */
   SHUTTING_DOWN,
   
   /**
	 * The segment acknowledged the shutdown signal with an explicit failure signal
	 */
	ABORTED,

	/**
	 * The segment has been signaled to abort immediately with no priority given to task completion.
	 */
   // ABORTING,
   
   /**
    * The segment's allotted time for an abort has passed without any indication of status
    */
   // NOT_RESPONDING,
   
   /**
    * The segment has completed an abort and released resources at an unknown progress cost.
    */
   CRASHED,
   
   /**
    * The segment completed a graceful shutdown and is safely inert.
    */
	GRACEFULLY_SHUTDOWN
}
