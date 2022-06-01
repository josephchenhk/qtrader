import threading
from threading import Thread

import win32event
import win32com.client
from win32com.client import constants
import pythoncom
import sys

win32com.client.gencache.EnsureModule('{51F35562-AEB4-4AB3-99E8-AC9666344B64}', 0, 4, 0)


def AssertMessage(condition, message):
    if not condition:
        raise RuntimeError(message)


def Trace(message):
    threadId = threading.current_thread().ident
    formattedMessage = str(threadId) + ': ' + message

    print(formattedMessage)
    sys.stdout.flush()


# Class implements ICQGCELEvent interface. All sink classes used in examples should be inherited from this class.
class CELSinkBase:
    def OnLineTimeChanged(self, newLineTime):
        pass

    def OnGWConnectionStatusChanged(self, newStatus):
        pass

    def OnDataConnectionStatusChanged(self, newStatus):
        pass

    def OnInstrumentSubscribed(self, symbol, cqgInstrument):
        pass

    def OnInstrumentChanged(self, cqgInstrument, cqgQuotes, cqgInstrumentProperties):
        pass

    def OnInstrumentDOMChanged(self, cqgInstrument, prevAsks, prevBids):
        pass

    def OnAccountChanged(self, changeType, cqgAccount, cqgPosition):
        pass

    def OnIsReady(self, readyStatus):
        pass

    def OnIdle(self):
        pass

    def OnDataError(self, cqgError, errorDescription):
        pass

    def OnCELStarted(self):
        pass

    def OnIncorrectSymbol(self, symbol):
        pass

    def OnCommodityInstrumentsResolved(self, commodityName, instrumentTypes, cqgCommodityInstruments):
        pass

    def OnGWEnvironmentChanged(self, eventCode, accountId, phase):
        pass

    def OnCurrencyRatesChanged(self, cqgCurrencyRates):
        pass

    def OnQueryProgress(self, cqgOrdersQuery, cqgError):
        pass

    def OnOrderChanged(self, changeType, cqgOrder, oldProperties, cqgFill, cqgError):
        pass

    def OnDataSourcesResolved(self, cqgDataSources, cqgError):
        pass

    def OnDataSourceSymbolsResolved(self, dataSourceAbbreviation, cqgDataSourceSymbols, cqgError):
        pass

    def OnCustomSessionsResolved(self, cqgSessionsCollection, cqgError):
        pass

    def OnTradableCommoditiesResolved(self, gwAccountId, cqgCommodities, cqgError):
        pass

    def OnTicksResolved(self, cqgTicks, cqgError):
        pass

    def OnTicksAdded(self, cqgTicks, added_ticks_count):
        pass

    def OnTicksRemoved(self, cqgTicks, removedTickIndex):
        pass

    def OnTimedBarsResolved(self, cqgTimedBars, cqgError):
        pass

    def OnTimedBarsAdded(self, cqgTimedBars):
        pass

    def OnTimedBarsUpdated(self, cqgTimedBars, index):
        pass

    def OnConstantVolumeBarsResolved(self, cqgConstantVolumeBars, cqgError):
        pass

    def OnConstantVolumeBarsAdded(self, cqgConstantVolumeBars):
        pass

    def OnConstantVolumeBarsUpdated(self, cqgConstantVolumeBars, index):
        pass

    def OnPointAndFigureBarsResolved(self, cqgPointAndFigureBars, cqgError):
        pass

    def OnPointAndFigureBarsAdded(self, cqgPointAndFigureBars):
        pass

    def OnPointAndFigureBarsUpdated(self, cqgPointAndFigureBars, index):
        pass

    def OnYieldsResolved(self, cqgYields, cqgError):
        pass

    def OnYieldsAdded(self, cqgYields):
        pass

    def OnYieldsUpdated(self, cqgYields, index):
        pass

    def OnTFlowBarsResolved(self, cqgTFlowBars, cqgError):
        pass

    def OnTFlowBarsAdded(self, cqgTFlowBars):
        pass

    def OnTFlowBarsUpdated(self, cqgTFlowBars, index):
        pass

    def OnCustomStudyDefinitionsResolved(self, cqgCustomStudyDefinitions, cqgError):
        pass

    def OnTradingSystemDefinitionsResolved(self, cqgCustomStudyDefinitions, cqgError):
        pass

    def OnConditionDefinitionsResolved(self, cqgConditionDefinitions, cqgError):
        pass

    def OnQFormulaDefinitionsResolved(self, cqgQFormulaDefinitions, cqgError):
        pass

    def OnCustomStudyResolved(self, cqgCustomStudy, cqgError):
        pass

    def OnCustomStudyAdded(self, cqgCustomStudy):
        pass

    def OnCustomStudyUpdated(self, cqgCustomStudy, index):
        pass

    def OnConditionResolved(self, cqgCondition, cqgError):
        pass

    def OnConditionAdded(self, cqgCondition):
        pass

    def OnConditionUpdated(self, cqgCondition, index):
        pass

    def OnTradingSystemResolved(self, cqgTradingSystem, cqgError):
        pass

    def OnTradingSystemAddNotification(self, cqgTradingSystem, cqgTradingSystemAddInfo):
        pass

    def OnTradingSystemUpdateNotification(self, cqgTradingSystem, cqgTradingSystemUpdateInfo):
        pass

    def OnExpressionResolved(self, cqgExpression, cqgError):
        pass

    def OnExpressionAdded(self, cqgExpression):
        pass

    def OnExpressionUpdated(self, cqgExpression, index):
        pass

    def OnAlgorithmicOrderRegistrationComplete(self, guid, cqgError):
        pass

    def OnAlgorithmicOrderPlaced(self, guid, mainParams, customProps):
        pass

    def OnTimedBarsInserted(self, cqgTimedBars, index):
        pass

    def OnTimedBarsRemoved(self, cqgTimedBars, index):
        pass

    def OnTonstantVolumeBarsInserted(self, cqgConstantVolumeBars, index):
        pass

    def OnTonstantVolumeBarsRemoved(self, cqgConstantVolumeBars, index):
        pass

    def OnPointAndFigureBarsInserted(self, cqgPointAndFigureBars, index):
        pass

    def OnPointAndFigureBarsRemoved(self, cqgPointAndFigureBars, index):
        pass

    def OnYieldsInserted(self, cqgYields, index):
        pass

    def OnYieldsRemoved(self, cqgYields, index):
        pass

    def OnTFlowBarsInserted(self, cqgTFlowBars, index):
        pass

    def OnTFlowBarsRemoved(self, cqgTFlowBars, index):
        pass

    def OnExpressionInserted(self, cqgExpression, index):
        pass

    def OnExpressionRemoved(self, cqgExpression, index):
        pass

    def OnTonditionInserted(self, cqgCondition, index):
        pass

    def OnTonditionRemoved(self, cqgCondition, index):
        pass

    def OnTustomStudyInserted(self, cqgCustomStudy, index):
        pass

    def OnTustomStudyRemoved(self, cqgCustomStudy, index):
        pass

    def OnICConnectionStatusChanged(self, newStatus):
        pass

    def OnAllOrdersCanceled(self, orderSide, gwAccountIds, instrumentNames):
        pass

    def OnInstrumentsGroupResolved(self, cqgInstrumentsGroup, cqgError):
        pass

    def OnInstrumentsGroupChanged(self, changeType, cqgInstrumentsGroup, instrumentsNames):
        pass

    def OnBarsTimestampsResolved(self, cqgBarsTimestamps, cqgError):
        pass

    def OnStrategyDefinitionProgress(self, cqgDefinition, cqgError):
        pass

    def OnTradingSystemInsertNotification(self, cqgTradingSystem, cqgTradingSystemInsertInfo):
        pass

    def OnTradingSystemRemoveNotification(self, cqgTradingSystem, cqgTradingSystemRemoveInfo):
        pass

    def OnTradingSystemTradeRelationAddNotification(self, cqgTradingSystem, cqgTradingSystemRelationAddInfo):
        pass

    def OnTradableExchangesResolved(self, gwAccountId, cqgExchanges, cqgError):
        pass

    def OnHistoricalSessionsResolved(self, cqgHistoricalSessions, cqgHistoricalSessionsRequest, cqgError):
        pass

    def OnSummariesStatementResolved(self, cqgSummariesStatement, cqgError):
        pass

    def OnPositionsStatementResolved(self, cqgPositionsStatement, cqgError):
        pass

    def OnManualFillUpdateResolved(self, cqgManualFillRequest, cqgError):
        pass

    def OnManualFillsResolved(self, cqgManualFills, cqgError):
        pass

    def OnManualFillChanged(self, cqgManualFill, modifyType):
        pass

    def OnAuthenticationStatusChanged(self, newStatus, cqgError):
        pass

    def OnPasswordChanged(self, requestStatus, cqgError):
        pass

    def OnAdvancedStudyDefinitionsResolved(self, cqgAdvancedStudyDefinitions, cqgError):
        pass

    def OnAdvancedStudyResolved(self, cqgAdvancedStudy, cqgError):
        pass

    def OnAdvancedStudyAdded(self, cqgAdvancedStudy):
        pass

    def OnAdvancedStudyUpdated(self, cqgAdvancedStudy, index):
        pass

    def OnAdvancedStudyInserted(self, cqgAdvancedStudy, index):
        pass

    def OnAdvancedStudyRemoved(self, cqgAdvancedStudy, index):
        pass

    def OnSubMinuteBarsResolved(self, cqgSubminuteBars, cqgError):
        pass

    def OnSubMinuteBarsAdded(self, cqgSubminuteBars):
        pass

    def OnSubMinuteBarsUpdated(self, cqgSubminuteBars, index):
        pass

    def OnSubMinuteBarsInserted(self, cqgSubminuteBars, index):
        pass

    def OnSubMinuteBarsRemoved(self, cqgSubminuteBars, index):
        pass

    def OnInstrumentResolved(self, symbol, cqgInstrument, cqgError):
        pass

    def OnStrategyQuoteRequestResolved(self, cqgRFQ, isProcessed, cqgError):
        pass


# Auxiliary class
class SinkInternal(CELSinkBase):
    def __init__(self):
        Trace("Internal sink ctor")

    def Init(self, eventCELStarted, celEnvironment):
        self.eventCELStarted = eventCELStarted
        self.celEnvironment = celEnvironment

    def OnCELStarted(self):
        Trace("SinkInternal: CELStarted!")
        self.eventCELStarted.set()

    def OnDataError(self, cqgError, errorDescription):
        if cqgError is not None:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace("SinkInternal: OnDataError: Code: {} Description: {}".format(dispatchedCQGError.Code,
                                                                               dispatchedCQGError.Description))

        self.celEnvironment.SetError()
        self.eventCELStarted.set()


# Class prepares environment for comfortable using of CQG API in python
class CELEnvironment:
    def __init__(self):
        self.eventCELStarted = threading.Event()
        self.eventAllPrepared = threading.Event()
        self.shutdownEvent = win32event.CreateEvent(None, 0, 0, None)
        self.errorHappened = False

    def Init(self, sinkType, customAPIConfiguration):
        self.thread = Thread(target=self.threadedFunction, args=(sinkType,), name="cel_threadfunc")
        self.thread.start()

        Trace("Waiting for CQGCEL creation..")
        self.eventAllPrepared.wait()
        Trace("CQGCEL is created!")

        Trace("Marshaled CQGCEL from stream")
        self.cqgCEL = win32com.client.Dispatch(
            pythoncom.CoGetInterfaceAndReleaseStream(self.cqgCELStream, pythoncom.IID_IDispatch))
        Trace("CQGCEL is marshaled from stream!")

        self.apiConfigurationSet(customAPIConfiguration)

        self.startupCel()

        if self.errorHappened:
            Trace("Error happened during CQGCEL start up!")

        return self.sink

    def threadedFunction(self, sinkType):
        Trace("COM framework is initializing...")
        pythoncom.CoInitialize()
        Trace("COM framework is initialized!")

        Trace("CQG.CQGCEL.4 creation...")
        cqgCEL = win32com.client.Dispatch("CQG.CQGCEL.4")
        Trace("CQG.CQGCEL.4 created!")

        Trace("Marshal CQGCEL in stream")
        self.cqgCELStream = pythoncom.CoMarshalInterThreadInterfaceInStream(pythoncom.IID_IDispatch, cqgCEL)
        Trace("CQGCEL is marshaled in stream")

        Trace("Internal sink subscription to events")
        sinkInternal = win32com.client.WithEvents(cqgCEL, SinkInternal)
        Trace("Internal sink subscribed")

        Trace("Sink subscription to events")
        self.sink = win32com.client.WithEvents(cqgCEL, sinkType)
        Trace("Sink subscribed")

        sinkInternal.Init(self.eventCELStarted, self)
        Trace("Internal sink is initialized")

        self.eventAllPrepared.set()

        Trace("Starting message pumping..")

        self.pumpMessages()

        Trace("COM framework is uninitializing...")
        pythoncom.CoUninitialize()
        Trace("COM framework is uninitialized!")

    def pumpMessages(self):

        while True:
            res = win32event.MsgWaitForMultipleObjects((self.shutdownEvent,), 0, win32event.INFINITE,
                                                       win32event.QS_ALLEVENTS)

            if res == win32event.WAIT_OBJECT_0:
                break
            elif res == win32event.WAIT_OBJECT_0 + 1:
                if pythoncom.PumpWaitingMessages():
                    break  # wm_quit
            else:
                raise RuntimeError("unexpected win32wait return value")

    def apiConfigurationSet(self, customAPIConfiguration):
        Trace("CQGCEL default configuring...")
        self.cqgCEL.APIConfiguration.CollectionsThrowException = False
        self.cqgCEL.APIConfiguration.NewInstrumentChangeMode = True
        self.cqgCEL.APIConfiguration.ReadyStatusCheck = 0
        self.cqgCEL.APIConfiguration.TimeZoneCode = constants.tzChina # constants.tzCentral
        self.cqgCEL.APIConfiguration.IncludeOrderTransactions = True
        self.cqgCEL.APIConfiguration.UseOrderSide = True
        # self.cqgCEL.APIConfiguration.NewInstrumentMode = True

        if customAPIConfiguration is not None:
            Trace("CQGCEL custom configuring...")
            customAPIConfiguration(self.cqgCEL.APIConfiguration)

    def startupCel(self):
        Trace("Startup CQGCEL..")
        self.cqgCEL.Startup()
        Trace("Waiting for CQGCEL start..")
        self.eventCELStarted.wait()
        Trace("CQGCEL has started.")

    def Shutdown(self):
        Trace("CQGCEL is being shut down..")
        self.cqgCEL.Shutdown()
        win32event.SetEvent(self.shutdownEvent)

        self.thread.join()

        Trace("CQGCEL is shut down.")

    def SetError(self):
        self.errorHappened = True


# Function prepares an environment for a sample starting
# sampleClassType is a class type (not the instance) that implements the sample
# customConfiguration is a function sets APIConfiguration properties in addition to CELEnvironment.apiConfigurationSet
def Start(sampleClassType, customConfiguration = None):
    celEnvironment = CELEnvironment()
    try:
        sample = celEnvironment.Init(sampleClassType, customConfiguration)
        if not celEnvironment.errorHappened:
            sample.Init(celEnvironment)
            sample.Start()
    except Exception as e:
        Trace("Exception: {}".format(str(e)))
    finally:
        celEnvironment.Shutdown()
