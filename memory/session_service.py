from google.adk.sessions import InMemorySessionService


class SalesSessionService:
    """
    Session manager using ADK's InMemorySessionService.
    Stores conversation history in RAM.
    """

    def __init__(self):
        # Initialize the ADK InMemorySessionService
        # This is the standard ADK way for prototyping without a DB
        self.service = InMemorySessionService()
        print("Session Service initialized (In-Memory)")

    def get_service(self):
        """Returns the ADK session service instance."""
        return self.service
