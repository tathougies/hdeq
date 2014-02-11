module Distributed.DEQ.Local(
  newLocalQueue,
  newRawLocalQueue,

  findQueue,
  findQueue'
  ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad.Fix
import Control.Monad.Error

import Distributed.DEQ.Types

import qualified Data.Map as M
import Data.Typeable

import System.Log.Logger

moduleName = "Distributed.DEQ.Local"

newLocalQueue :: DEQable a => DEQ -> QueueName -> Visibility -> IO (Either QueueError (WriteEnd a, ReadEnd a))
newLocalQueue deq name vis = mfix $ \(~(Right (typ, _))) ->
  atomically $ runErrorT $ do
    q <- lift $ findQueue' deq name
    case q of
      Just q -> throwError (QueueAlreadyExists name vis)
      Nothing -> do
        chan <- lift newBroadcastTChan -- This reference is stored in our global queue map, and is always consumed from
        chan' <- lift (dupTChan chan) -- This is the actual reference we return
        let queue = QueueInfo {
              qiName = name,
              qiVisibility = vis,
              qiType = fromTypeRep reflectedType }
            reflectedType = head . typeRepArgs . typeOf $ typ
            readEnd = ReadEnd queue chan'
            writeEnd = WriteEnd queue chan
        lift $ modifyTVar (deqQueues deq) (M.insert name (DEQQueue queue chan))
        return (writeEnd, readEnd)

newRawLocalQueue :: DEQ -> QueueInfo -> IO (Either QueueError (WriteEnd Raw, ReadEnd Raw))
newRawLocalQueue deq qi = do
  infoM moduleName $ concat ["Creating raw queue ", show (qiName qi)]
  atomically $ runErrorT $ do
      q <- lift $ findQueue' deq (qiName qi)
      case q of
        Just q -> throwError (QueueAlreadyExists (qiName qi) (qiVisibility qi))
        Nothing -> do
          chan <- lift newBroadcastTChan
          chan' <- lift (dupTChan chan)
          lift $ modifyTVar (deqQueues deq) (M.insert (qiName qi) (DEQQueue qi chan))
          return (WriteEnd qi chan, ReadEnd qi chan')

findQueue :: DEQ -> QueueName -> IO (Maybe DEQQueue)
findQueue deq name = atomically (findQueue' deq name)

findQueue' :: DEQ -> QueueName -> STM (Maybe DEQQueue)
findQueue' deq name = M.lookup name <$> readTVar (deqQueues deq)