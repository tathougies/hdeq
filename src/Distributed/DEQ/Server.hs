{-# LANGUAGE DoAndIfThenElse, ExistentialQuantification, RankNTypes #-}
module Distributed.DEQ.Server (
  serverStart
  ) where

import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Resource
import Control.Concurrent hiding (yield)
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMChan

import qualified Data.Map as M
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.Conduit
import Data.Conduit.TMChan
import Data.Conduit.Network.Unix as Unix
import Data.Conduit.Network as Tcp
import Data.Monoid
import Data.Maybe
import Data.Serialize hiding (Fail)
import Data.Word

import Distributed.DEQ.Queue
import Distributed.DEQ.Config
import Distributed.DEQ.Types
import Distributed.DEQ.Local
import Distributed.DEQ.Remote

import System.Log.Logger
import System.Random

import Unsafe.Coerce

moduleName = "Distributed.DEQ.Server"
msgQueueSize = 4096

data DEQConnection = DEQConnection
    { connDeq      :: DEQ
    , connSettings :: DEQSettings
    , connType     :: HostType
    , connSource   :: Source IO ByteString
    , connSink     :: Sink ByteString IO ()
    , connEndpoint :: String }

data ServerRWEnd = forall a. DEQable a => ServerWriteEnd (WriteEnd a)

data ServerState = ServerState
    { ssTokens       :: M.Map QueueToken ServerRWEnd
    , ssOutputChan   :: TBMChan Response
    }

emptyState :: ServerState
emptyState = ServerState M.empty undefined

serverStart :: DEQ -> DEQSettings -> IO ()
serverStart deq settings = infoM moduleName "Starting server..." >>
                           case sHostType settings of
                             UNIX -> runUnixServer (Unix.serverSettings
                                                    (sHostUnixSocket settings))
                                                   (unixDeqServer deq settings)
                             TCP -> runTCPServer (Tcp.serverSettings
                                                  (sHostPort settings)
                                                  (sHostName settings))
                                                 (tcpDeqServer deq settings)

tcpDeqServer :: DEQ -> DEQSettings -> Tcp.Application IO
tcpDeqServer deq settings appData = deqServer
                                    (DEQConnection {
                                       connDeq      = deq,
                                       connSettings = settings,
                                       connType     = TCP,
                                       connSource   = Tcp.appSource appData,
                                       connSink     = Tcp.appSink appData,
                                       connEndpoint = show (Tcp.appSockAddr appData)})

unixDeqServer :: DEQ -> DEQSettings -> Unix.Application IO
unixDeqServer deq settings appData  = infoM moduleName "UNIX client starting..." >>
                                      deqServer
                                      (DEQConnection {
                                          connDeq      = deq,
                                          connSettings = settings,
                                          connType     = UNIX,
                                          connSource   = Unix.appSource appData,
                                          connSink     = Unix.appSink appData,
                                          connEndpoint = sHostUnixSocket settings})

deqServer :: DEQConnection -> IO ()
deqServer conn = startServer
    where startServer = runResourceT $ do
            output <- liftIO $ newTBMChanIO msgQueueSize -- This TChan allows us to multiplex the output between the main server and the data streaming stuff
            let st = emptyState { ssOutputChan = output }
            liftIO (forkIO (runResourceT (transPipe liftIO (sourceTBMChan output) $= encodeResponse $$ transPipe liftIO (connSink conn)))) -- Run the output server...
            runResourceT (transPipe liftIO (connSource conn) $= parseMessage $= server st $$ transPipe liftIO (sinkTBMChan output True))

          dataServer chan tok rd = forever $ do
            infoM moduleName $ "Waiting for data..."
            what <- receive rd
            case what of
              Left err -> warningM moduleName $ concat ["Error while reading in dataServer", show err]
              Right x -> do
                infoM moduleName $ "Sending " ++ show (encode x)
                atomically (writeTBMChan chan (DataR tok (encode x)))

          server st = await >>= \msg ->
                      case msg of
                        Nothing -> return ()
                        Just msg ->do
                                 st' <- case msg of
                                          Join qn qt -> doJoin qn qt st
                                          Leave qt -> unimplemented st

                                          Info qn -> unimplemented st
                                          Stream de -> unimplemented st
                                          Register qi -> doRegister qi st
                                          Unregister qt -> unimplemented st

                                          Data qt dat -> doData qt dat st

                                          Ping -> unimplemented st
                                 server st'

          unimplemented st = do
            yield (Fail (OtherError "Unimplemented"))
            return st

          newQueueToken = QueueToken <$> randomIO

          newToken :: DEQable b => ServerState -> (forall a. DEQable a => ReadEnd a -> ServerRWEnd) -> ReadEnd b -> IO (QueueToken, ServerState)
          newToken st cons q = do
            token <- liftIO newQueueToken
            let ssTokens' = M.insert token newRWEnd (ssTokens st)
                newRWEnd  = cons q -- Type was already checked...
            return (token, st { ssTokens = ssTokens' })

          doData qt dat st = do
            liftIO (infoM moduleName $ concat ["Data ", show qt, " ", show dat])
            case M.lookup qt (ssTokens st) of
              Nothing -> do
                liftIO (warningM moduleName $ concat ["Ignoring data sent to ", show qt, ": Queue token not found"])
                return st
              Just (ServerWriteEnd wr) -> case decode dat of
                Left err -> do
                  liftIO (warningM moduleName $ concat ["Error decoding in Data: ", err])
                  return st
                Right x -> do
                  liftIO (infoM moduleName ("Sending data... " ++ show x))
                  liftIO (send x wr)
                  return st

          doRegister qi st = do
            liftIO (infoM moduleName (concat ["Registering ", show (qiName qi)]))
            q <- liftIO (newRawQueue (connDeq conn) qi)
            case q of
              Left err -> do
                liftIO (warningM moduleName (concat ["Error during registration: ", show err]))
                yield (Fail err)
                return st
              Right (wr, _) -> do
                token <- liftIO newQueueToken
                liftIO (warningM moduleName (concat ["Registered ", show token, " :: ", show (qiType qi)]))
                let ssTokens' = M.insert token (ServerWriteEnd wr) (ssTokens st)
                yield (Registered token qi)
                return (st { ssTokens = ssTokens' })

          doJoin qn qt st = do
            q <- liftIO $ findQueue (connDeq conn) qn
            case q of
              Nothing -> do
                     q <- liftIO $ clientJoinRawQueue (connDeq conn) qn
                     case q of
                       Left err -> do
                              yield (Fail err)
                              return st
                       Right q@(ReadEnd qi c) -> do
                              tok <- liftIO newQueueToken
                              q' <- ReadEnd qi <$> (liftIO $ atomically $ dupTChan c)
                              allocate (forkIO (dataServer (ssOutputChan st) tok q')) killThread
                              yield (Joined tok qi)
                              return st
              Just (DEQQueue qi q)
                  | isNothing qt || fromJust qt == qiType qi -> do
                              tok <- liftIO newQueueToken
                              q' <- ReadEnd qi <$> (liftIO $ atomically $ dupTChan q)
                              allocate (forkIO (dataServer (ssOutputChan st) tok q')) killThread
                              yield (Joined tok qi)
                              return st
                  | otherwise -> do
                     yield (Fail TypeMismatch)
                     return st

          encodeResponse = do
            resp <- await
            liftIO (infoM moduleName $ "Sending response: " ++ show resp)
            case resp of
              Nothing -> return ()
              Just resp -> do
                let respS = encode resp
                    lengthS = encode ((fromIntegral . BS.length $ respS) :: Word16)
                liftIO (putStrLn $ "Response: " ++ show resp ++ " " ++ show respS)
                yield (lengthS <> respS)
                encodeResponse

-- main :: IO ()
-- main = do
--   settings <- loadDEQServerSettings

--   deqState <- newDEQServerState settings

--   runTCPServer (serverSettings
--                 (sPort settings)
--                 (sHostname settings))
--                (deqServer deqState)