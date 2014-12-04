from ryu.base import app_manager
from ryu.controller import (dpset,
                            event,
                            handler,
                            network,
                            ofp_event,
                            conf_switch)
from ryu.lib import quantum_ifaces, ofctl_v1_0
from ryu.app.quantum_adapter import (OVSSwitch, OVSPort)
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.ofproto import nx_match, ofproto_v1_0_parser, ofproto_v1_0
from ryu.lib import hub
import threading

DEFAULT_TIMEOUT=5.0
flow_stats_pool = []

def update_dscp_thread(datapath, dscp_value, ofport, waiters):
    t = threading.Thread(target=update_dscp, args = (datapath, dscp_value, ofport, waiters))
    t.daemon = True
    t.start()

def update_dscp(datapath, dscp_value, ofport, waiters):
    flow = {}
    flows = get_flow_stats(datapath, waiters, flow)
    for flow in flows[str(datapath.id)]:
        actions = flow['actions']
        match = flow['match']
        
        # Only flow with some actions and with in_port passed from api
        if match.in_port == ofport and actions:
            new_actions = []
            
            #Add dscp rule
            #http://sourceforge.net/p/ryu/mailman/message/29612110/
            #dscp value must be multiple of 4.
            #beacause only 6 bits are admitted and the last two must be 0
            nw_tos = int(dscp_value)
            dscp_action = ofproto_v1_0_parser.OFPActionSetNwTos(nw_tos)
            new_actions.append(dscp_action)
            
            # Add others actions different from dscp
            for action in actions:
                # Add all actions except for dscp
                if (action.cls_action_type == ofproto_v1_0.OFPAT_VENDOR) and (action.cls_subtype != ofproto_v1_0.OFPAT_SET_NW_TOS):
                    new_actions.append(action)
     
            mod = datapath.ofproto_parser.OFPFlowMod(
                datapath=datapath, match=match, cookie=0,
                command=ofproto_v1_0.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
                priority=ofproto_v1_0.OFP_DEFAULT_PRIORITY,
                flags=ofproto_v1_0.OFPFF_SEND_FLOW_REM, actions=new_actions)
            datapath.send_msg(mod)
    flow_stats_pool.append(flows)
    return flows

def send_flow_mod(self, dp, rule, table, command, priority, actions):
    command = self._make_command(table, command)
    dp.send_flow_mod(rule=rule, cookie=self.DEFAULT_COOKIE,
                     command=command, idle_timeout=0,
                     hard_timeout=0, priority=priority, actions=actions)
    

def get_flow_stats(dp, waiters, flow={}):
    match = ofctl_v1_0.to_match(dp, flow.get('match', {}))
    table_id = int(flow.get('table_id', 0xff))
    out_port = int(flow.get('out_port', dp.ofproto.OFPP_NONE))

    stats = dp.ofproto_parser.OFPFlowStatsRequest(
        dp, 0, match, table_id, out_port)

    msgs = []
    ofctl_v1_0.send_stats_request(dp, stats, waiters, msgs)

    flows = []
    for msg in msgs:
        for stats in msg.body:
            actions = ofctl_v1_0.actions_to_str(stats.actions)
            match = ofctl_v1_0.match_to_str(stats.match)

            s = {'actions': stats.actions,
                 'match': stats.match}
            flows.append(s)
    flows = {str(dp.id) : flows}
    return flows

class QoS(app_manager.RyuApp):
    
    _CONTEXTS = {
        'network': network.Network,
        'dpset': dpset.DPSet,
        'quantum_ifaces': quantum_ifaces.QuantumIfaces,
        'conf_switch': conf_switch.ConfSwitchSet,
    }
    
    temp_response = {}
    
    class EventRateLimitPort(event.EventBase):
        def __init__(self, port_id, key, value):
            super(QoS.EventRateLimitPort, self).__init__()
            self.port_id = port_id
            self.key = key
            self.value = value
         
        def __str__(self):
            return ('EventRateLimitPort')
        
    class EventDscpPort(event.EventBase):
        def __init__(self, dpid, name, ofport, dscp_value):
            super(QoS.EventDscpPort, self).__init__()
            self.dpid = dpid
            self.name = name
            self.ofport = ofport
            self.dscp_value = dscp_value
         
        def __str__(self):
            return ('EventDscpPort')
    
    def __init__(self, *args, **kwargs):
        super(QoS, self).__init__()
        self.nw = kwargs['network']
        self.dpset = kwargs['dpset']
        #self.tunnels = kwargs['tunnels']
        self.ifaces = kwargs['quantum_ifaces']
        self.dps = {}
        self.waiters = {}
        
        map(lambda ev_cls: self.register_observer(ev_cls, self.name),
            [QoS.EventDscpPort, QoS.EventRateLimitPort])
        
    def _get_ovs_switch(self, dpid, create=True):
        ovs_switch = self.dps.get(dpid)
        if not ovs_switch:
            if create:
                ovs_switch = OVSSwitch(self.CONF, dpid, self.nw, self.ifaces,
                                       self.logger)
                self.dps[dpid] = ovs_switch
        else:
            self.logger.debug('ovs switch %s is already known', dpid)
        return ovs_switch
    
    @handler.set_ev_handler(EventRateLimitPort)
    def rate_limit_handler(self, ev):
        in_out = ev.key
        port_to_update = "qvo" + ev.port_id[:11]
        for dp in self.dpset.get_all(): # dp is a tuple of type: (dpid_A, Datapath_A). ryu.controller.controller.Datapath
            dpid = dp[1].id
            for port in self.dpset.get_ports(dpid): # port 
                if port.name == port_to_update:
                    ovs = self._get_ovs_switch(dpid, False)
                    if not ovs:
                        print "Error: impossible to found OVS instance with dpid %s" % dpid
                        return
                    else:
                        print "OVS found. update rate_limit. qos_cn"
                        ovs.ovs_bridge.add_rate_limit(port_to_update, ev.value, in_out)
                    return
        print "port %s not found in any ovs bridge! " % port_to_update

 
    @handler.set_ev_handler(EventDscpPort)
    def dscp_handler(self, ev):
        dpid = ev.dpid
        ovs = self._get_ovs_switch(dpid, False)
        if not ovs:
            print "Error: impossible to found OVS instance with dpid %s" % ev.dpid
            return
        else:
            print "OVS found. update dscp"
            datapath = self.dpset.get(dpid)
            if datapath:
                dscp_value = ev.dscp_value
                ofport = ev.ofport
                update_dscp_thread(datapath, dscp_value, ofport, self.waiters)
            else:
                print "Error. No datapath found"
            
    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(ofproto.OFPFW_ALL, 0, 0, 0,
                                    0, 0, 0, 0, 0, 0, 0, 0, 0)
        stats = parser.OFPFlowStatsRequest(datapath, 0, match,
                                               0xff, ofproto.OFPP_NONE)
        
        msgs = []
        print "invio"
        datapath.send_msg(stats)
        print "inviato"
        self.waiters.setdefault(datapath.id, {})
        lock = hub.Event()
        self.waiters[stats.xid] = (lock, msgs)
         
        try:
            lock.wait(timeout=5.0)
        except hub.Timeout:
            del self.waiters[stats.xid]
        return self.waiters[stats.xid]
        
        
    @handler.set_ev_cls(dpset.EventDP)
    def dp_handler(self, ev):
        dpid = ev.dp.id
        ovs_switch = self._get_ovs_switch(dpid, True) #add ovs instance
        if not ovs_switch:
            return
        
        if ev.enter:
            for port in ev.ports:
                ovs_switch.update_port(port.port_no, port.name, True)
        else:
            self.dps.pop(dpid, None)
            
    @handler.set_ev_cls(conf_switch.EventConfSwitchSet)
    def conf_switch_set_handler(self, ev):
        if ev.key == 'ovsdb_addr':
            ovs_switch = self._get_ovs_switch(ev.dpid)
            ovs_switch.set_ovsdb_addr(ev.dpid, ev.value)
            #self._conf_switch_set_ovsdb_addr(ev.dpid, ev.value)
        else:
            self.logger.debug("unknown event: %s", ev)
            
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def stats_reply_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
   
        if dp.id not in self.waiters:
            return
        if msg.xid not in self.waiters[dp.id]:
            return
        lock, msgs = self.waiters[dp.id][msg.xid]
        msgs.append(msg)
          
        print lock, dp.id, msg.xid
   
        flags = 0
        if dp.ofproto.OFP_VERSION == ofproto_v1_0.OFP_VERSION:
            flags = dp.ofproto.OFPSF_REPLY_MORE
        elif dp.ofproto.OFP_VERSION == ofproto_v1_2.OFP_VERSION:
            flags = dp.ofproto.OFPSF_REPLY_MORE
        elif dp.ofproto.OFP_VERSION == ofproto_v1_3.OFP_VERSION:
            flags = dp.ofproto.OFPMPF_REPLY_MORE
   
        if msg.flags & flags:
            return
        del self.waiters[dp.id][msg.xid]
        lock.set()