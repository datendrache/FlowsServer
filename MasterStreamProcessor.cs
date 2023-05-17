//   Phloz
//   Copyright (C) 2003-2019 Eric Knight

using System;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Runtime.ExceptionServices;
using FatumCore;
using System.Data.SQLite;
using System.Collections;
using PhlozLib;
using DatabaseAdapters;
using PhlozLib.SearchCore;
using Proliferation.LanguageAdapters;
using System.Linq;

namespace PhlozCore
{
    public class MasterStreamProcessor
    {
        public CollectionState State = null;
        private Dispatcher theDispatcher = null;
        private Einstein einstein = null;
 
        Thread serverThread = null;
        Thread heartThread = null;
        Thread taskThread = null;

        Boolean DailyRolloverLock = false;
        DateTime RolloverDay = DateTime.Now;
        public Boolean forcerollover = false;
        public Boolean serverRunning = false;
        public Boolean webRunning = false;
        public Boolean heartRunning = false;
        public Boolean dispatchRunning = false;
        public Boolean taskRunning = false;
        DateTime flowUpdateAutomaticTimer = DateTime.Now;
        DateTime databaseUpdateAutomaticTimer = DateTime.Now;

        public MasterStreamProcessor(fatumconfig FC)
        {
            State = new CollectionState(FC);
            State.MasterReceiver = new BaseReceiver(new DocumentEventHandler(DocumentArrived),
                new PhlozLib.ErrorEventHandler(ErrorReceived),
                new EventHandler(CommunicationLost),
                new EventHandler(StoppedReceiver),
                new FlowEventHandler(onFlowDetected));
            CollectionState.getState(State, FC);
        }

        public void Start()
        {
            // These must be started in order of necessity
            try
            {
                if (einstein == null)
                {
                    einstein = new Einstein(State, this);
                }

                if (theDispatcher == null)
                {
                    theDispatcher = new Dispatcher(State, new DocumentEventHandler(DocumentArrived));
                    theDispatcher.startDispatch();
                }

                if (serverThread == null)
                {
                    serverThread = new System.Threading.Thread(launch);
                    serverThread.Start();
                }

                while (!serverRunning)
                {
                    Thread.Sleep(50);
                }

                if (heartThread == null)
                {
                    heartThread = new System.Threading.Thread(HeartBeat);
                    heartThread.Start();
                }

                if (taskThread == null)
                {
                    taskThread = new System.Threading.Thread(TaskProcessing);
                    taskThread.Start();
                }

                while (!heartRunning)
                {
                    Thread.Sleep(50);
                }

                while (!webRunning)
                {
                    Thread.Sleep(50);
                }
            }
            catch (Exception xyz)
            {
                int i = 0;
            }
        }


        public void Stop()
        {
            theDispatcher.stopDispatch();
            heartThread.Abort();
            BaseCommand.systemcollectionstop(State);
            serverThread.Abort();
        }

        private void launch()
        {
            State.Started = DateTime.Now;
            string dbdirectory = State.config.GetProperty("StatisticsDirectory");
            if (File.Exists(dbdirectory + "\\statistics.s3db"))
            {
                State.statsDB = new DatabaseAdapters.SQLiteDatabase(dbdirectory + "\\statistics.s3db");
            }
            else
            {
                if (!Directory.Exists(dbdirectory))
                {
                    Directory.CreateDirectory(dbdirectory);
                }

                SQLiteConnection.CreateFile(dbdirectory + "\\statistics.s3db");
                State.statsDB = new DatabaseAdapters.SQLiteDatabase(dbdirectory + "\\statistics.s3db");
                BaseFlowStatus.defaultSQL(State.statsDB, DatabaseSoftware.SQLite);
            }
            serverRunning = true;
        }

        private void CommunicationLost(object source, EventArgs e)
        {
            ReceiverInterface receiver = (ReceiverInterface)source;
            errorMessage("General Processing", "Error", receiver.getReceiverType() + " Service stopped communicating, attempting to restart.");
            receiver.Start(); // Attempt to restart receiver
        }

        private void StoppedReceiver(object source, EventArgs e)
        {
            ReceiverInterface receiver = (ReceiverInterface)source;
            errorMessage("General Processing", "Information", receiver.getReceiverType() + " Receiver shut down.");
        }

        private void ErrorReceived(object source, PhlozLib.ErrorEventArgs e)
        {
            errorMessage("General Processing", "Error", e.ErrorMessage);
        }

        public void DocumentArrived(Object o, DocumentEventArgs e)
        {
            State.DocumentCount++;

            if (e.Document.assignedFlow == null)
            {
                e.Document.assignedFlow = BaseFlow.locateCachedFlowByUniqueID(e.Document.FlowID, State);
                if (e.Document.assignedFlow == null)
                {
                    e.Document.assignedFlow = State.logFlow;  // Default this to the error stream
                    e.Document.FlowID = e.Document.assignedFlow.UniqueID;
                }
                else
                {
                    e.Document.FlowID = e.Document.assignedFlow.UniqueID;
                }
            }

            if (!e.Document.assignedFlow.Suspended)
            {
                if (e.Document.assignedFlow.Parameter!=null)
                {
                    if (e.Document.assignedFlow.Parameter.ExtractedMetadata != null)
                    {
                        if (e.Document.assignedFlow.Parameter.ExtractedMetadata.tree.Count > 0)
                        {
                            Tree.mergeNode(e.Document.assignedFlow.Parameter.ExtractedMetadata, e.Document.Metadata);
                        }
                    }
                }

                if (e.Document.assignedFlow.ParentService!=null)
                {
                    if (e.Document.assignedFlow.ParentService.Parameter.ExtractedMetadata != null)
                    {
                        if (e.Document.assignedFlow.ParentService.Parameter.ExtractedMetadata.tree.Count > 0)
                        {
                            Tree.mergeNode(e.Document.assignedFlow.ParentService.Parameter.ExtractedMetadata, e.Document.Metadata);
                        }
                    }

                    if (e.Document.assignedFlow.ParentService.ParentSource != null)
                    {
                        if (e.Document.assignedFlow.ParentService.ParentSource.Parameter.ExtractedMetadata != null)
                        {
                            if (e.Document.assignedFlow.ParentService.ParentSource.Parameter.ExtractedMetadata.tree.Count > 0)
                            {
                                Tree.mergeNode(e.Document.assignedFlow.ParentService.ParentSource.Parameter.ExtractedMetadata, e.Document.Metadata);
                            }
                        }
                    }
                }

                if (e.Document.assignedFlow.ProcessingEnabled)
                {
                    if (einstein!=null)
                    {
                        einstein.processDocument(e.Document.assignedFlow, e.Document, State);
                    }
                }

                State.LabelStats.recordDocument(e.Document);
                State.CategoryStats.recordDocument(e.Document);

                if (e.Document.assignedFlow.RetainDocuments)
                {
                    BaseDocument.insertDocumentBulk(State, e.Document);
                }
                if (theDispatcher!=null)
                {
                    theDispatcher.dispatch_document(e.Document);
                }
            }
        }

        [HandleProcessCorruptedStateExceptions]
        private void HeartBeat()
        {
            heartRunning = true;
            while (heartRunning)
            {
                Boolean didNothing = true;
                Thread.Sleep(0);

                try
                {
                    foreach (BaseSource currentSource in State.Sources)
                    {
                        foreach (BaseService currentService in currentSource.Services)
                        {
                            foreach (BaseFlow currentFlow in currentService.Flows)
                            {
                                if (!currentFlow.Suspended)
                                {
                                    if (currentFlow.bulkInsert!=null)
                                    {
                                        if (BaseFlow.BulkInsert(State, currentFlow))
                                        {
                                            didNothing = true;
                                        }
                                    }

                                    if (currentFlow.documentDB != null)
                                    {
                                        if (!currentFlow.documentDB.GetTransactionLockStatus())
                                        {
                                            try
                                            {
                                                currentFlow.documentDB.Commit();
                                            }
                                            catch (Exception xyz)
                                            {
                                                errorMessage("Master Stream Processor", "Error", "Commit failed on flow "+ currentFlow.UniqueID + "/" + currentFlow.FlowName + ": " + xyz.Message);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if ((flowUpdateAutomaticTimer.Ticks + 50000000) < DateTime.Now.Ticks)
                    {
                        flowUpdateAutomaticTimer = DateTime.Now;
                        State.updateState();
                    }

                    if ((databaseUpdateAutomaticTimer.Ticks + 600000000) < DateTime.Now.Ticks)
                    {
                        databaseUpdateAutomaticTimer = DateTime.Now;
                        if (State.searchSystem!=null)
                        {
                            State.searchSystem.updateDatabases();
                        }
                    }
                }
                catch (Exception xyz)
                {
                    errorMessage("Master Stream Processor", "Error", "Heartbeat processing failed - uncaught exception: " + xyz.Message);
                }

                try
                {
                    foreach (BaseSource currentSource in State.Sources)
                    {
                        foreach (BaseService currentService in currentSource.Services)
                        {
                            foreach (ReceiverInterface recv in currentService.Receivers)
                            {
                                recv.MSPHeartBeat();
                            }
                        }
                    }
                }
                catch (Exception xyz)
                {
                    errorMessage("Master Stream Processor", "Error", "Flow Status Heartbeat Failed - uncaught exception: " + xyz.Message);
                }

                try
                {
                    //  At this point we change days
                    if (RolloverDay.Day != DateTime.Now.Day || forcerollover)
                    {
                        RollOver();
                    }
                }
                catch (Exception xyz)
                {
                    errorMessage("Master Stream Processor", "Error", "Day Change Processing Failed - uncaught exception: " + xyz.Message);
                }

                if (didNothing) Thread.Sleep(100);  // Might as well take a 1/10th of a second chill pill, things aren't active.
            }
        }

        private void onFlowDetected(Object o, FlowEventArgs fe)
        {
            BaseFlow currentFlow = fe.Flow;

            BaseFlow.enableFlow(State, currentFlow);
            if (currentFlow.Enabled == true)
            {
                BaseService currentService = null;
                foreach (BaseSource source in State.Sources)
                {
                    if (source.UniqueID == currentFlow.ParentService.ParentSource.UniqueID)
                    {
                        foreach (BaseService service in source.Services)
                        {
                            if (service.UniqueID == currentFlow.ParentService.UniqueID)
                            {
                                currentService = service;
                                break;
                            }
                        }
                        break;
                    }
                }

                if (currentService != null)
                {
                    currentService.Flows.Add(currentFlow);
                }
            }
        }



        public void errorMessage(string errLabel, string errCategory, string errMessage)
        {
            BaseDocument errorMessage = new BaseDocument(State.alarmFlow);
            errorMessage.assignedFlow = State.alarmFlow;
            errorMessage.FlowID = State.alarmFlow.UniqueID;
            errorMessage.received = DateTime.Now;
            errorMessage.Label = errLabel;
            errorMessage.Category = errCategory;
            errorMessage.Document = errMessage;
            errorMessage.Metadata = new Tree();
            BaseDocument.insertDocumentBulk(State, errorMessage);
            theDispatcher.redirect_document(errorMessage);
        }

        private void RollOver()
        {
            if (DailyRolloverLock == false)
            {
                DailyRolloverLock = true;
                // Update Today's Date
                RolloverDay = DateTime.Now;

                string dbdirectory = State.config.GetProperty("DocumentDatabaseDirectory");

                // If drive space is low, delete unarchived files.

                string archivefolder = State.config.GetProperty("ArchiveDirectory");
                archivefolder += "\\";
                if (!Directory.Exists(archivefolder))
                {
                    Directory.CreateDirectory(archivefolder);
                }

                var lastFile = Directory.EnumerateFiles(archivefolder).OrderByDescending(filename => filename);

                foreach (DriveInfo drive in DriveInfo.GetDrives())
                {
                    if (drive.RootDirectory.FullName.Substring(0, 2) == archivefolder.Substring(0, 2))  // Locate archive drive
                    {
                        foreach (string filename in lastFile)  // Check, Loop, Delete if above 79% capacity
                        {
                            long allspace = drive.TotalSize / (1024 * 1024);
                            long freespace = drive.TotalFreeSpace / (1024 * 1024);
                            long usedspace = allspace - freespace;
                            int percent = (int)((double)((double)usedspace / (double)allspace) * 100);
                            if (percent > 79)
                            {
                                try
                                {
                                    File.Delete(filename);
                                }
                                catch (Exception xyz)
                                {
                                    int ire = 0;  // Something is blocking the archive from being deleted.
                                }
                            }
                        }
                    }
                }
                
                foreach (BaseSource currentSource in State.Sources)
                {
                    foreach (BaseService currentService in currentSource.Services)
                    {
                        foreach (BaseFlow currentFlow in currentService.Flows)
                        {
                            currentFlow.Suspend();

                            if (currentFlow.documentDB != null)
                            {
                                if (currentFlow.documentDB.GetTransactionLockStatus())
                                {
                                    try
                                    {
                                        currentFlow.documentDB.Commit();
                                    }
                                    catch (Exception)
                                    {

                                    }
                                }
                                currentFlow.documentDB.Close();
                            }

                            if (currentFlow.indexer != null)
                            {
                                currentFlow.indexer.Close();
                                currentFlow.indexer = null;
                            }

                            if (currentFlow.RetainDocuments)
                            {
                                currentFlow.initializeDatabase(dbdirectory, DateTime.Now);
                            }

                            if (currentFlow.IndexString)
                            {
                                currentFlow.initializeIndex(dbdirectory, DateTime.Now);
                            }
                            if (currentFlow.FlowStatus == null) BaseFlowStatus.loadBaseFlowStatus(State, currentFlow);
                            currentFlow.Resume();
                        }
                    }
                }

                // Launch a thread that locates, compresses and moves all old database files *********************************

                try
                {
                    State.searchSystem.closeDatabases();
                }
                catch (Exception xyz)
                {
                    errorMessage("Master Stream Processor", "Critical", "Searching Indexes could not be closed.  Error details: " + xyz.Message);
                }

                // Lowest Priority -- retire old databases...

                try
                {
                    // Retire all old databases...

                    DateTime lastweek = DateTime.Now;
                    lastweek = lastweek.AddHours(7 * (-24));
                    ArrayList notLoaded = State.searchSystem.loadDatabases(lastweek);
                    State.archiveOldData(notLoaded);
                    //notLoaded.Clear();
                }
                catch (Exception xyz)
                {
                    errorMessage("Master Stream Processor", "Critical", "Archiving old data failed.  Error details: " + xyz.Message);
                }
                DailyRolloverLock = false;
            }
        }

        private void TaskProcessing()
        {
            System.Timers.Timer TaskTimer = new System.Timers.Timer(1000);
            TaskTimer.Elapsed += TaskTimerHeartBeat;
            TaskTimer.Interval = 60000;
            TaskTimer.Enabled = true;
            TaskTimer.AutoReset = true;
        }

        private void TaskTimerHeartBeat(Object source, System.Timers.ElapsedEventArgs e)
        {
            if (taskRunning == false)
            {
                taskRunning = true;
                try
                {
                    foreach (BaseTask currentTask in State.TaskList)
                    {
                        if (BaseTask.executeReady(currentTask))
                        {
                            currentTask.lastrun = DateTime.Now;
                            Tree SearchResults = null;

                            if (currentTask.runtime == null)
                            {
                                BaseProcessor currentProcessor = BaseProcessor.getProcessorByUniqueID(State.managementDB, currentTask.ProcessorID);
                                if (currentProcessor.Enabled.ToLower() == "true")
                                {
                                    IntLanguage runtime = null;
                                    string CompilationOutput = "";

                                    switch (currentProcessor.Language)
                                    {
                                        case "Flowish":
                                            runtime = new ContainerFlowish();
                                            runtime.initialize(currentProcessor.ProcessCode, out CompilationOutput);
                                            runtime.setCallback(new EventHandler(collectExternalDocument));
                                            break;
                                    }
                                }
                            }

                            string tmpOutput = "";
                            if (currentTask.query != null)
                            {
                                if (currentTask.query.leafnames.Count>0)
                                {
                                    SearchResults = BaseTask.performTaskQuery(State, currentTask);
                                    if (SearchResults == null)
                                    {
                                        SearchResults = new Tree();
                                    }

                                    if (currentTask.runtime!=null)
                                    {
                                        currentTask.runtime.addVariable("documents", SearchResults);
                                        currentTask.runtime.execute(out tmpOutput);
                                    }
                                    
                                    if (currentTask.forwarderLinks!=null)
                                    {
                                        foreach (ForwarderLink link in currentTask.forwarderLinks)
                                        {
                                            foreach (Tree document in SearchResults.tree)
                                            {
                                                BaseDocument newDocument = new BaseDocument((BaseFlow)null);
                                                newDocument.Document = document.getElement("Document");
                                                newDocument.Category = document.getElement("Category");
                                                newDocument.Label = document.getElement("Label");
                                                newDocument.received = new DateTime(long.Parse(document.getElement("Received")));
                                                newDocument.Metadata = document.Duplicate();
                                                newDocument.FlowID = currentTask.UniqueID;
                                                newDocument.assignedFlow = null;
                                                theDispatcher.dispatch_document(newDocument,currentTask.forwarderLinks);
                                            }
                                        }
                                    }
                                    
                                    SearchResults.dispose();
                                    SearchResults = null;
                                }
                            }
                        }
                    }
                }
                catch (Exception xyz)
                {
                    errorMessage("Master Stream Processor", "Error", "Task Execution Failed.  Error details: " + xyz.Message + ", " + xyz.StackTrace);
                }
                taskRunning = false;
            }
        }

        private void collectExternalDocument(Object o, EventArgs e)
        {
            ArrayList document = (ArrayList)o;
            BaseDocument newDocument = new BaseDocument(document, State);
            DocumentEventArgs cbe = new DocumentEventArgs();
            cbe.Document = newDocument;
            DocumentArrived(this, cbe);
            document.Clear();
        }
    }
}
