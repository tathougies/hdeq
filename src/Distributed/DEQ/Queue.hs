{-# LANGUAGE OverloadedStrings #-}
module Distributed.DEQ.Queue (
  newDEQ,
  newQueue,
  newRawQueue,
  joinQueue,
  send,
  receive,
  serveDEQ
  ) where

import {-# SOURCE #-} Distributed.DEQ.Server
import Distributed.DEQ.Types
import Distributed.DEQ.Remote
import Distributed.DEQ.Local
import Distributed.DEQ.Config

import Control.Concurrent
import Control.Applicative
import Control.Monad.Error
import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Serialize

import System.Log.Logger

moduleName = "Distributed.DEQ.Queue"

-- | Creates a new root DEQ environment
newDEQ :: DEQSettings -> IO DEQ
newDEQ settings = mfix $ \deq -> do
                    when (not (sHostDisabled settings)) $ do
                      forkIO (serverStart deq settings)
                      return ()
                    DEQ <$> newTVarIO M.empty
                        <*> (if sClientDisabled settings
                             then (putStrLn "Client disabled" *> pure Nothing)
                             else Just <$> clientStart deq settings)
                        <*> pure settings

-- | Creates a new queue with the given name. Can fail if the new queue has the same name as
--   another in its visibility level.
newQueue :: DEQable a => DEQ -> QueueName -> Visibility -> IO (Either QueueError (WriteEnd a, ReadEnd a))
newQueue deq name vis = newQueue'
  where newQueue' =
          case vis of
            Local 0 -> newLocalQueue deq name vis
            _
              | isRootServer deq -> newLocalQueue deq name vis
              | otherwise -> clientCreateQueue deq name vis

newRawQueue :: DEQ -> QueueInfo -> IO (Either QueueError (WriteEnd Raw, ReadEnd Raw))
newRawQueue deq qi = do
  infoM moduleName $ concat ["newRawQueue ", show (qiName qi), " vis: ", show (qiVisibility qi)]
  case qiVisibility qi of
    Local 0 -> newRawLocalQueue deq qi
    _
      | isRootServer deq -> newRawLocalQueue deq qi
      | otherwise -> clientCreateRawQueue deq qi

joinQueue :: DEQable a => DEQ -> QueueName -> IO (Either QueueError (ReadEnd a))
joinQueue deq name = do
  q <- atomically $ findQueue' deq name
  case q of
    Nothing -> clientJoinQueue deq name
    Just q -> do
      rd <- getReadEnd q
      case rd of
        Nothing -> return (Left TypeMismatch)
        Just rd -> return (Right rd)


send :: DEQable a => a -> WriteEnd a -> IO (Either QueueError ())
send x (WriteEnd _ chan) = Right <$> atomically (writeTChan chan x)

receive :: DEQable a => ReadEnd a -> IO (Either QueueError a)
receive (ReadEnd _ chan) = Right <$> atomically (readTChan chan)

serveDEQ deq = do
  r <- newQueue deq "/.deq/quit" (Local 0)
  case r of
    Left err -> fail ("Could not serveDEQ: " ++ show err)
    Right (_, q)  -> do
                 r <- receive q
                 case r of
                   Left err -> fail ("Got a message from receive: " ++ show err)
                   Right () -> return ()