{-# LANGUAGE ExistentialQuantification #-}
module Distributed.DEQ.Remote (
  clientCreateQueue,
  clientCreateRawQueue,

  clientJoinQueue,
  clientJoinRawQueue,
  clientStart,
  parseMessage) where

import Distributed.DEQ.Types
import Distributed.DEQ.Config

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMChan
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Fix
import Control.Monad.Trans.Resource
import Control.Monad.Trans

import qualified Data.Map as M
import qualified Data.ByteString as BS
import qualified Data.Conduit.List as CL
import Data.Typeable
import Data.Word
import Data.Monoid
import Data.Serialize hiding (Fail)
import Data.String
import Data.Conduit
import Data.Conduit.Network.Unix as Unix
import Data.Conduit.Network as Tcp
import Data.Conduit.TMChan
import Data.ByteString (ByteString)

import System.IO
import System.Log.Logger

moduleName = "Distributed.DEQ.Remote"
queueSize = 4096

data DEQConnection = DEQConnection
                     { connDeq :: DEQ
                     , connSettings :: DEQSettings
                     , connSource :: Source IO ByteString
                     , connSink :: Sink ByteString IO ()
                     , connEndpoint :: String
                     , connCommands :: TBMChan (Command, MVar LocalResponse) }

data ClientState = ClientState
                   { csQueues :: M.Map QueueToken (TChan ByteString),
                     csOutput :: TBMChan Command }

-- | Length of size messages that precede message itself
serverMsgSizeWordSize :: Word16
serverMsgSizeWordSize = 2

emptyState = ClientState M.empty undefined

clientRequest :: DEQ -> Command -> IO (Either QueueError LocalResponse)
clientRequest DEQ { deqClient = Nothing } _ = return (Left NotConnectedToRemote)
clientRequest (DEQ { deqClient = Just chan }) cmd = do
  responseVar <- newEmptyMVar
  atomically (writeTBMChan chan (cmd, responseVar))
  Right <$> readMVar responseVar

clientCreateQueue :: DEQable a => DEQ -> QueueName -> Visibility
                     -> IO (Either QueueError (WriteEnd a, ReadEnd a))
clientCreateQueue DEQ { deqClient = Nothing } _ _ = return (Left NotConnectedToRemote)
clientCreateQueue _ _ (Local 0) = error "clientCreateQueue: Called to create queue with local visibility"
clientCreateQueue deq name vis = do
  infoM moduleName $ concat ["createClientQueue ", show name, " ", show vis]
  mfix $ \(~(Right (wrEnd, _))) ->
    case deqClient deq of
      Nothing -> return (Left NotConnectedToRemote)
      Just chan -> do
       let qi = QueueInfo {
              qiName       = name,
              qiVisibility = vis,
              qiType       = fromTypeRep reflectedType }
           reflectedType = head . typeRepArgs . typeOf $ wrEnd
       response <- clientRequest deq (Register qi)
       case response of
         Left e -> return (Left e)
         Right (LocalQueueRegistered qi chan) -> do
           chan' <- convertTChan chan
           chan'' <- atomically $ dupTChan chan'
           return (Right (WriteEnd qi chan', ReadEnd qi chan''))
         Right (LocalRemoteFailure err) -> return (Left err)
         Right response -> return (Left UnexpectedReply)

clientJoinQueue :: DEQable a => DEQ -> QueueName -> IO (Either QueueError (ReadEnd a))
clientJoinQueue DEQ { deqClient = Nothing } _ = return (Left NotConnectedToRemote)
clientJoinQueue deq name = mfix $ \(~(Right rd)) -> do
  let reflectedType = fromTypeRep . head . typeRepArgs . typeOf $ rd
  response <- clientRequest deq (Join name (Just reflectedType))
  case response of
    Left e -> return (Left e)
    Right (LocalQueueJoined qi chan) -> do
      chan' <- convertTChan chan
      return (Right (ReadEnd qi chan'))
    Right (LocalRemoteFailure err) -> return (Left err)
    Right _ -> return (Left UnexpectedReply)

clientJoinRawQueue :: DEQ -> QueueName -> IO (Either QueueError (ReadEnd Raw))
clientJoinRawQueue deq qn =
  case deqClient deq of
    Nothing -> return (Left NotConnectedToRemote)
    Just chan -> do
      response <- clientRequest deq (Join qn Nothing) -- this is a raw join
      case response of
        Left e -> return (Left e)
        Right (LocalQueueJoined qi chan) -> do
          chan' <- mkRawTChan chan
          return (Right (ReadEnd qi chan'))
        Right response -> return (Left UnexpectedReply)

clientCreateRawQueue :: DEQ -> QueueInfo -> IO (Either QueueError (WriteEnd Raw, ReadEnd Raw))
clientCreateRawQueue deq qi = return (Left (OtherError "unimplemented"))

-- TODO: We need to figure out how to get rid of these when we're done...
mkRawTChan :: TChan ByteString -> IO (TChan Raw)
mkRawTChan chan = do
  chan' <- newTChanIO
  forkIO . forever . atomically $ (readTChan chan >>= (writeTChan chan' . Raw))
  forkIO . forever . atomically $ (readTChan chan' >>= (writeTChan chan . unRaw))
  return chan'

convertTChan :: DEQable a => TChan ByteString -> IO (TChan a)
convertTChan chan = mfix $ \(~t) -> do
  let typeRep = head . typeRepArgs . typeOf $ t
  chan' <- newTChanIO
  forkIO . forever $ do
    writeStatus <- atomically $ do
      x <- readTChan chan
      let x' = decode x
      case x' of
        Left s -> return (Left s)
        Right x' -> do
          writeTChan chan' x'
          return (Right ())

    -- If there was a decoding error, we output a warning.
    case writeStatus of
      Left err -> warningM moduleName (concat ["Could not decode ", show typeRep,
                                               " from queue: ", err])
      Right () -> return ()
  forkIO . forever . atomically $ do
    x <- readTChan chan'
    let x' = encode x
    writeTChan chan x'
  return chan'

clientStart :: DEQ -> DEQSettings -> IO (TBMChan (Command, MVar LocalResponse))
clientStart deq settings = do
  infoM moduleName "Starting client"
  chan <- newTBMChanIO queueSize
  forkIO $ case sClientType settings of
    UNIX -> runUnixClient (Unix.clientSettings
                           (sClientUnixSocket settings))
                          (unixDeqClient deq settings chan)
    TCP -> runTCPClient (Tcp.clientSettings
                        (sClientPort settings)
                        (fromString (sClientHostName settings)))
                       (tcpDeqClient deq settings chan)
  return chan

unixDeqClient :: DEQ -> DEQSettings -> TBMChan (Command, MVar LocalResponse) -> Unix.Application IO
unixDeqClient deq settings chan appData = unixClient (DEQConnection {
                                                         connDeq = deq,
                                                         connSettings = settings,
                                                         connSource = Unix.appSource appData,
                                                         connSink = Unix.appSink appData,
                                                         connEndpoint = sClientUnixSocket settings,
                                                         connCommands = chan})

tcpDeqClient :: DEQ -> DEQSettings -> TBMChan (Command, MVar LocalResponse) -> Tcp.Application IO
tcpDeqClient deq settings chan appData = unixClient (DEQConnection {
                                                        connDeq = deq,
                                                        connSettings = settings,
                                                        connSource = Tcp.appSource appData,
                                                        connSink = Tcp.appSink appData,
                                                        connEndpoint = show (Tcp.appSockAddr appData),
                                                        connCommands = chan})

unixClient :: DEQConnection -> IO ()
unixClient conn = runClient
  where runClient = do
          infoM moduleName "Starting client..."
          response <- newEmptyMVar -- Where the response from the read end comes in
          output <- newTBMChanIO queueSize
          forkIO (runResourceT (sourceTBMChan (connCommands conn) $= issueCommands response $$ sinkTBMChan output True))
          forkIO (runResourceT (sourceTBMChan output $= encodeCommands $$ transPipe liftIO (connSink conn)))
          let st = emptyState { csOutput = output }
          runResourceT (transPipe liftIO (connSource conn) $= parseMessage $$ sendResponses response st)

        issueCommands response =
          await >>= \x ->
          case x of
            Nothing -> return ()
            Just (cmd, ret) -> do
              yield cmd
              liftIO (takeMVar response >>= putMVar ret) -- Move from response to ret...
              issueCommands response

        encodeCommands =
          await >>= \cmd ->
          case cmd of
            Nothing -> return ()
            Just cmd -> do
              let encoded = encode cmd
                  len = encode (fromIntegral (BS.length encoded) :: Word16)
              yield (len <> encoded)
              encodeCommands

        dataServer qt output chan = forever $ atomically $ do
          d <- readTChan chan
          writeTBMChan output (Data qt d)

        sendResponses response st =
          await >>= \resp ->
          case resp of
            Nothing -> return ()

            -- Check if the response means anything special...
            Just (DataR qt dat) -> do
              case M.lookup qt (csQueues st) of
                Nothing -> do
                  liftIO (warningM moduleName $ concat ["Data sent to unknown token: ", show qt])
                  sendResponses response st
                Just q -> do
                  liftIO (infoM moduleName $ concat ["dataReceived: ", show dat])
                  liftIO (atomically $ writeTChan q dat)
                  sendResponses response st
            Just (Joined tok qi) -> do
              chan <- liftIO newTChanIO
              let csQueues' = M.insert tok chan (csQueues st)
              liftIO (putMVar response (LocalQueueJoined qi chan))
              sendResponses response (st { csQueues = csQueues' })
            Just (Registered qt qi) -> do
              chan <- liftIO newTChanIO
              -- This launches the thread that reads from this new queue and sends it to the server.
              allocate (forkIO (dataServer qt (csOutput st) chan)) killThread
              liftIO (putMVar response (LocalQueueRegistered qi chan))
              sendResponses response st
            Just (Fail err) -> do
              liftIO (warningM moduleName $ concat ["Request returned error: ", show err])
              liftIO (putMVar response (LocalRemoteFailure err))
              sendResponses response st
            Just _ -> do
              liftIO (warningM moduleName "Unknown response received")
              sendResponses response st

-- Utility Functions

parseMessage :: (MonadIO m, Serialize a) => Conduit ByteString m a
parseMessage = parseMessage' id 0

parseMessages :: (MonadIO m, Serialize a) => ByteString -> Conduit ByteString m a
parseMessages inStream
  | BS.length inStream == 0 = parseMessage
  | BS.length inStream < fromIntegral serverMsgSizeWordSize = do
      new <- await
      case new of
        Nothing -> return ()
        Just x -> parseMessages (inStream <> x)
  | otherwise = do
      let (sizeS, rest) = BS.splitAt (fromIntegral serverMsgSizeWordSize) inStream
          Right size    = decode sizeS :: Either String Word16 -- This really shouldn't fail...
          (actualData, rest') = BS.splitAt (fromIntegral size) rest
      if BS.length rest >= fromIntegral size
        then case decode actualData of
               Left err -> badFormat err
               Right x -> do
                 liftIO (infoM moduleName $ "Received: " ++ show actualData)
                 yield x
                 parseMessages rest'
        else parseMessage' (rest:) (size - fromIntegral (BS.length rest))

parseMessage' :: (MonadIO m, Serialize a) => ([ByteString] -> [ByteString]) -> Word16 -> Conduit ByteString m a
parseMessage' chunks leftInChunk = do
  msg <- await
  case msg of
    Nothing -> return ()
    Just msg -> do
      let (old, new) = BS.splitAt (fromIntegral leftInChunk) msg
          chunks' = chunks . (old:)
      when ((sum . map BS.length $ chunks' []) > 0) $
        case (decode . BS.concat $ chunks' []) of
          Left err -> badFormat err
          Right x -> do
            liftIO (infoM moduleName $ "Received: " ++ show (BS.concat $ chunks' []))
            yield x
      parseMessages new

badFormat err = liftIO $ warningM moduleName $ concat ["Couldn't decode message: ", err]