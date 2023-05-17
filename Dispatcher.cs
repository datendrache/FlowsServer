//   Phloz
//   Copyright (C) 2003-2019 Eric Knight

using System;
using FatumCore;
using System.Collections;
using System.Runtime.ExceptionServices;
using PhlozLib;

namespace PhlozCore
{
    public class Dispatcher
    {
        CollectionState State = null;
        DocumentEventHandler msgHandler = null;
        System.Timers.Timer SystemHeartBeat = null;

        public Dispatcher(CollectionState S, DocumentEventHandler MH)
        {
            State = S;
            msgHandler = MH;
        }

        public void startDispatch()
        {
            SystemHeartBeat = new System.Timers.Timer(60000);
            SystemHeartBeat.Elapsed += new System.Timers.ElapsedEventHandler(HeartBeatCallBack);
            SystemHeartBeat.Enabled = true;
            SystemHeartBeat.Start();
        }

        public void stopDispatch()
        {
            foreach (BaseForwarder current in State.Forwarders)
            {
                try
                {
                    if (!current.FailedToInit)
                    {
                        if (current.ForwarderState != null)
                        {
                            ForwarderInterface forwarder = (ForwarderInterface)current.ForwarderState;
                            forwarder.HeartBeat();  // Clean it out one last time
                            forwarder.Stop();
                            forwarder.Dispose();
                        }
                    }
                }
                catch (Exception xyz)
                {
                    TextLog.Log(State.fatumConfig.DBDirectory + "\\system.log", DateTime.Now.ToString() + ":" + xyz.Message + "\r\nStack Trace:\r\n" + xyz.StackTrace);
                    BaseDocument newAlarm = new BaseDocument(State.logFlow);
                    newAlarm.Document = "Dispatcher Heart Beat exception: " + xyz.Message + ", " + xyz.StackTrace;
                    newAlarm.received = DateTime.Now;
                    newAlarm.FlowID = State.logFlow.UniqueID;
                    newAlarm.Label = "Forwarder";
                    newAlarm.Category = "Heart Beat Error";
                    newAlarm.assignedFlow = null;
                    DocumentEventArgs newAlarmArgs = new DocumentEventArgs();
                    newAlarmArgs.Document = newAlarm;

                    redirect_document(newAlarm);
                }
            }
        }

        public void redirect_document(BaseDocument message)
        {
            Boolean forwardcheck = true;

            // Step one:  prevent infinite loop

            if (message.assignedFlow != null)
            {
                if (message.assignedFlow.UniqueID == message.FlowID)
                {
                    if ((message.FlowID != State.logFlow.UniqueID) && (message.FlowID != State.alarmFlow.UniqueID))
                    {
                        forwardcheck = false;

                        BaseDocument newAlarm = new BaseDocument(State.logFlow);
                        newAlarm.Document = "Dispatcher blocked infinite loop for flow " + message.assignedFlow.ParentService.ServiceType + "." + message.assignedFlow.ParentService.ServiceSubtype + "." + message.assignedFlow.FlowName + " [" + message.assignedFlow.UniqueID + "]";
                        newAlarm.received = DateTime.Now;
                        newAlarm.FlowID = State.logFlow.UniqueID;
                        newAlarm.Label = "Receiver";
                        newAlarm.Category = "Restarting";
                        newAlarm.assignedFlow = null;
                        DocumentEventArgs newAlarmArgs = new DocumentEventArgs();
                        newAlarmArgs.Document = newAlarm;

                        redirect_document(newAlarm);
                    }
                }
                else
                {
                    message.assignedFlow = null;  // strip it -- no longer needed.
                }
            }

            if (forwardcheck)
            {
                // Step two:  Forward message
                if (msgHandler != null)
                {
                    DocumentEventArgs newMsg = new DocumentEventArgs();
                    newMsg.Document = message;
                    msgHandler.Invoke(newMsg.Document.assignedFlow, newMsg);
                }
            }
        }

        public void dispatch_document(BaseDocument message) 
        {
            FlowReference flowinfo = (FlowReference) message.assignedFlow.flowReference;
            if (flowinfo!=null)
            {
                if (flowinfo.ForwarderLinks!=null)
                {
                    foreach (ForwarderLink currentLink in flowinfo.ForwarderLinks)
                    {
                        BaseForwarder current = BaseForwarder.locateForwarder(State.Forwarders, currentLink.ForwarderID);
                        if (current!=null)
                        {
                            if (!current.FailedToInit)
                            {
                                if (current.ForwarderState != null)
                                {
                                    ForwarderInterface value = (ForwarderInterface)current.ForwarderState;
                                    BaseDocument newMessageIncarnation = message.copy();
                                    value.sendDocument(newMessageIncarnation);
                                }
                                else
                                {
                                    ForwarderInterface value = initializeForwarder(current);
                                    if (value != null)
                                    {
                                        current.ForwarderState = value;
                                        BaseDocument newMessageIncarnation = message.copy();
                                        value.sendDocument(newMessageIncarnation);
                                    }
                                    else
                                    {
                                        current.FailedToInit = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            //if (State.Channels!=null)
            //{
            //    if (State.Channels.Count>0)
            //    {
            //        foreach (BaseChannel channel in State.Channels)
            //        {
            //            Boolean DocumentAdded = false;
            //            foreach (ChannelFlow channelFlow in State.ChannelFlows)
            //            {
            //                if (!DocumentAdded)
            //                {
            //                    if ((channelFlow.FlowID == message.assignedFlow.UniqueID) && (channelFlow.ChannelID == channel.UniqueID))
            //                    {
            //                        lock (channel.Documents)
            //                        {
            //                            BaseDocument channelMsg = message.copy();
            //                            channelMsg.ID = message.ID;
            //                            channel.Documents.AddFirst(channelMsg);
            //                            DocumentAdded = true;
            //                            while (channel.Documents.Count > 100)
            //                            {
            //                                channel.Documents.RemoveLast();
            //                            }
            //                        }
            //                    }
            //                }
            //            }
            //        }
            //    }
            //}
        }

        public void dispatch_document(BaseDocument message, ArrayList forwarderLinks)
        {
            foreach (ForwarderLink currentLink in forwarderLinks)
            {
                BaseForwarder current = BaseForwarder.locateForwarder(State.Forwarders, currentLink.ForwarderID);
                if (current != null)
                {
                    if (!current.FailedToInit)
                    {
                        if (current.ForwarderState != null)
                        {
                            ForwarderInterface value = (ForwarderInterface)current.ForwarderState;
                            BaseDocument newMessageIncarnation = message.copy();
                            value.sendDocument(newMessageIncarnation);
                        }
                        else
                        {
                            ForwarderInterface value = initializeForwarder(current);
                            if (value != null)
                            {
                                current.ForwarderState = value;
                                BaseDocument newMessageIncarnation = message.copy();
                                value.sendDocument(newMessageIncarnation);
                            }
                            else
                            {
                                current.FailedToInit = true;
                            }
                        }
                    }
                }
            }
        }

        public ForwarderInterface initializeForwarder(BaseForwarder forwarder)
        {
            ForwarderInterface value = null;

            switch (forwarder.forwarderType)
            {
                case "file":   // COMPLETED
                    {
                        value = new FwdFile(forwarder);
                        value.Start();
                    }
                    break;
                case "syslog udp":  // COMPLETED
                    {
                        value = new FwdUDP(forwarder);
                        value.Start();
                    }
                    break;
                case "syslog tcp": // COMPLETED
                    {
                        value = new FwdTCP(forwarder);
                        value.Start();
                    }
                    break;
                case "smtp":
                    {
                        value = new FwdEmail(forwarder);
                        value.Start();
                    }
                    break;
                case "snmp":
                    {
                        value = new FwdUDP(forwarder);
                        value.Start();
                    }
                    break;
                 case "database":
                    {
                        value = new FwdUDP(forwarder);
                        value.Start();
                    }
                    break;
                case "application":
                    {
                        value = new FwdUDP(forwarder);
                        value.Start();
                    }
                    break;
                case "flow":  // In Progress
                    {
                        BaseFlow flow = BaseFlow.locateCachedFlowByUniqueID(forwarder.Parameters.ExtractedMetadata.getElement("flowid"), State);
                        if (flow != null)
                        {
                            value = new FwdFlow(forwarder, flow);
                            value.Start();
                        }
                    }
                    break;
                case "eventstream":
                    {
                        value = new FwdEventStream(forwarder);
                        value.Start();
                    }
                    break;
                case "tcpxml":
                    {
                        value = new FwdTCPXML(forwarder);
                        value.Start();
                    }
                    break;
            }
            return value;
        }

        [HandleProcessCorruptedStateExceptions]
        private void HeartBeatCallBack(Object o, System.Timers.ElapsedEventArgs e)
        {
            foreach (BaseForwarder current in State.Forwarders)
            {
                try
                {
                    if (!current.FailedToInit)
                    {
                        if (current.ForwarderState != null)
                        {
                            ForwarderInterface forwarder = (ForwarderInterface)current.ForwarderState;
                            forwarder.HeartBeat();
                        }
                    }
                }
                catch (Exception xyz)
                {
                    TextLog.Log(State.fatumConfig.DBDirectory + "\\system.log", DateTime.Now.ToString() + ":" + xyz.Message + "\r\nStack Trace:\r\n" + xyz.StackTrace);
                    BaseDocument newAlarm = new BaseDocument(State.alarmFlow);
                    newAlarm.Document = "Dispatcher Heart Beat exception: " + xyz.Message + ", " + xyz.StackTrace;
                    newAlarm.received = DateTime.Now;
                    newAlarm.FlowID = State.alarmFlow.UniqueID;
                    newAlarm.Label = "Forwarder";
                    newAlarm.Category = "Heart Beat Error";
                    DocumentEventArgs newAlarmArgs = new DocumentEventArgs();
                    newAlarmArgs.Document = newAlarm;

                    redirect_document(newAlarm);
                }
            }
        }
    }
}
