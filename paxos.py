'''
This module provides an implementation of the Paxos algorithm as
a set of composable classes.
'''

import collections

ProposalID = collections.namedtuple('ProposalID', ['number', 'uid'])

class PaxosMessage (object):
    from_uid = None

class Prepare (PaxosMessage):
    def __init__(self, from_uid, proposal_id):
        self.from_uid    = from_uid
        self.proposal_id = proposal_id

class Nack (PaxosMessage):
    def __init__(self, from_uid, proposer_uid, proposal_id, promised_proposal_id):
        self.from_uid             = from_uid
        self.proposal_id          = proposal_id
        self.proposer_uid         = proposer_uid
        self.promised_proposal_id = promised_proposal_id

class Promise (PaxosMessage):
    def __init__(self, from_uid, proposer_uid, proposal_id, last_accepted_id, last_accepted_value):
        self.from_uid             = from_uid
        self.proposer_uid         = proposer_uid
        self.proposal_id          = proposal_id
        self.last_accepted_id     = last_accepted_id
        self.last_accepted_value  = last_accepted_value

class Accept (PaxosMessage):
    def __init__(self, from_uid, proposal_id, proposal_value):
        self.from_uid       = from_uid
        self.proposal_id    = proposal_id
        self.proposal_value = proposal_value

class Accepted (PaxosMessage):
    def __init__(self, from_uid, proposal_id, proposal_value):
        self.from_uid       = from_uid
        self.proposal_id    = proposal_id
        self.proposal_value = proposal_value

class Resolution (PaxosMessage):
    def __init__(self, from_uid, value):
        self.from_uid = from_uid
        self.value    = value

class InvalidMessageError (Exception):
    pass

class MessageHandler (object):
    def receive(self, msg):
        handler = getattr(self, 'receive_' + msg.__class__.__name__.lower(), None)
        if handler is None:
            raise InvalidMessageError('Receiving class does not support messages of type: ' + msg.__class__.__name__)
        return handler(msg)

class Proposer (MessageHandler):
    leader               = False
    proposed_value       = None
    proposal_id          = None
    highest_accepted_id  = None
    promises_received    = None
    nacks_received       = None
    current_prepare_msg  = None
    current_accept_msg   = None

    def __init__(self, network_uid, quorum_size):
        self.network_uid         = network_uid
        self.quorum_size         = quorum_size
        self.proposal_id         = ProposalID(0, network_uid)
        self.highest_proposal_id = ProposalID(0, network_uid)

    def propose_value(self, value):
        if self.proposed_value is None:
            self.proposed_value = value
            if self.leader:
                self.current_accept_msg = Accept(self.network_uid, self.proposal_id, value)
                return self.current_accept_msg

    def prepare(self):
        self.leader              = False
        self.promises_received   = set()
        self.nacks_received      = set()
        self.proposal_id         = ProposalID(self.highest_proposal_id.number + 1, self.network_uid)
        self.highest_proposal_id = self.proposal_id
        self.current_prepare_msg = Prepare(self.network_uid, self.proposal_id)
        return self.current_prepare_msg

    def observe_proposal(self, proposal_id):
        if proposal_id > self.highest_proposal_id:
            self.highest_proposal_id = proposal_id

    def receive_nack(self, msg):
        self.observe_proposal(msg.promised_proposal_id)
        if msg.proposal_id == self.proposal_id and self.nacks_received is not None:
            self.nacks_received.add(msg.from_uid)
            if len(self.nacks_received) == self.quorum_size:
                return self.prepare()

    def receive_promise(self, msg):
        self.observe_proposal(msg.proposal_id)
        if not self.leader and msg.proposal_id == self.proposal_id and msg.from_uid not in self.promises_received:
            self.promises_received.add(msg.from_uid)
            if msg.last_accepted_id > self.highest_accepted_id:
                self.highest_accepted_id = msg.last_accepted_id
                if msg.last_accepted_value is not None:
                    self.proposed_value = msg.last_accepted_value
            if len(self.promises_received) == self.quorum_size:
                self.leader = True
                if self.proposed_value is not None:
                    self.current_accept_msg = Accept(self.network_uid, self.proposal_id, self.proposed_value)
                    return self.current_accept_msg

class Acceptor (MessageHandler):
    def __init__(self, network_uid, promised_id=None, accepted_id=None, accepted_value=None):
        self.network_uid    = network_uid
        self.promised_id    = promised_id
        self.accepted_id    = accepted_id
        self.accepted_value = accepted_value

    def receive_prepare(self, msg):
        if self.promised_id is None or msg.proposal_id >= self.promised_id:
            self.promised_id = msg.proposal_id
            return Promise(self.network_uid, msg.from_uid, self.promised_id, self.accepted_id, self.accepted_value)
        else:
            return Nack(self.network_uid, msg.from_uid, msg.proposal_id, self.promised_id)

    def receive_accept(self, msg):
        if self.promised_id is None or msg.proposal_id >= self.promised_id:
            self.promised_id     = msg.proposal_id
            self.accepted_id     = msg.proposal_id
            self.accepted_value  = msg.proposal_value
            return Accepted(self.network_uid, msg.proposal_id, msg.proposal_value)
        else:
            return Nack(self.network_uid, msg.from_uid, msg.proposal_id, self.promised_id)

class Learner (MessageHandler):
    class ProposalStatus (object):
        __slots__ = ['accept_count', 'retain_count', 'acceptors', 'value']
        def __init__(self, value):
            self.accept_count = 0
            self.retain_count = 0
            self.acceptors    = set()
            self.value        = value

    def __init__(self, network_uid, quorum_size):
        self.network_uid       = network_uid
        self.quorum_size       = quorum_size
        self.proposals         = dict()
        self.acceptors         = dict()
        self.final_value       = None
        self.final_acceptors   = None
        self.final_proposal_id = None

    def receive_accepted(self, msg):
        if self.final_value is not None:
            if msg.proposal_id >= self.final_proposal_id and msg.proposal_value == self.final_value:
                self.final_acceptors.add(msg.from_uid)
            return Resolution(self.network_uid, self.final_value)

        last_pn = self.acceptors.get(msg.from_uid)

        if last_pn is not None and msg.proposal_id <= last_pn:
            return

        self.acceptors[msg.from_uid] = msg.proposal_id

        if last_pn is not None:
            ps = self.proposals[last_pn]
            ps.retain_count -= 1
            ps.acceptors.remove(msg.from_uid)
            if ps.retain_count == 0:
                del self.proposals[last_pn]

        if not msg.proposal_id in self.proposals:
            self.proposals[msg.proposal_id] = Learner.ProposalStatus(msg.proposal_value)

        ps = self.proposals[msg.proposal_id]

        assert msg.proposal_value == ps.value, 'Value mismatch for single proposal!'

        ps.accept_count += 1
        ps.retain_count += 1
        ps.acceptors.add(msg.from_uid)

        if ps.accept_count == self.quorum_size:
            self.final_proposal_id = msg.proposal_id
            self.final_value       = msg.proposal_value
            self.final_acceptors   = ps.acceptors
            self.proposals         = None
            self.acceptors         = None
            return Resolution(self.network_uid, self.final_value)

class PaxosInstance (Proposer, Acceptor, Learner):
    def __init__(self, network_uid, quorum_size, promised_id=None, accepted_id=None, accepted_value=None):
        Proposer.__init__(self, network_uid, quorum_size)
        Acceptor.__init__(self, network_uid, promised_id, accepted_id, accepted_value)
        Learner.__init__(self, network_uid, quorum_size)

    def receive_prepare(self, msg):
        self.observe_proposal(msg.proposal_id)
        return super(PaxosInstance,self).receive_prepare(msg)

    def receive_accept(self, msg):
        self.observe_proposal(msg.proposal_id)
        return super(PaxosInstance,self).receive_accept(msg)
