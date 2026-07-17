class GCONNetwork:
    """
    Simulated GCON network.
    """

    def __init__(self, network=None):
        self.network = network
        
    def send_job(self, command):
        """
        Send a job through the network.
        """

        return self.dispatcher.dispatch(command)