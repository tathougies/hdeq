{-# LANGUAGE GeneralizedNewtypeDeriving, DeriveGeneric, ExistentialQuantification, DeriveDataTypeable #-}
module Distributed.DEQ.Types
       (QueueName(..),
        QueueType(..),
        QueueToken(..),
        QueueInfo(..),

        DEQ(..),

        Command(..),
        Response(..),
        LocalResponse(..),

        Visibility(..),

        ReadEnd(..),
        WriteEnd(..),

        QueueError(..),

        DEQable(..),
        Raw(..),
        fromTypeRep,
        isRootServer,
        getReadEnd,

        -- Internal
        DEQQueue(..)) where

import Distributed.DEQ.Config

import Control.Applicative
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TBMChan
import Control.Concurrent.MVar
import Control.Concurrent.Chan
import Control.Monad.Error
import Control.Monad
import Control.Monad.Fix

import qualified Data.Map as M
import qualified Data.ByteString as BS
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Data.ByteString (ByteString)
import Data.Serialize
import Data.Typeable
import Data.String
import Data.Word

import GHC.Generics

import Unsafe.Coerce

newtype QueueName = QueueName { unQueueName :: Text }
                  deriving (Show, Read, Eq, Ord, IsString)

newtype QueueType = QueueType { unQueueType :: Text }
                  deriving (Show, Read, Eq, Ord, IsString)

newtype QueueToken = QueueToken { unQueueToken :: Word64 }
                   deriving (Show, Read, Eq, Ord, Serialize)

instance Serialize QueueName where
  put (QueueName n) = put . encodeUtf8 $ n
  get               = QueueName . decodeUtf8 <$> get

instance Serialize QueueType where
  put (QueueType n) = put . encodeUtf8 $ n
  get               = QueueType . decodeUtf8 <$> get

data DEQQueue = forall a. DEQable a => DEQQueue QueueInfo (TChan a)

data DEQ = DEQ
             { deqQueues :: TVar (M.Map QueueName DEQQueue)
             , deqClient :: Maybe (TBMChan (Command, MVar LocalResponse))
             , deqSettings :: DEQSettings
             }

-- | Gives the visibility of a queue.
data Visibility = Global
                  -- ^ Global queues are propagated throughout the network.
                | Local Int
                  -- ^ Local queues will not be propagated more than n levels up in the hierarchy
                | Secret ByteString
                  -- ^ Secret queues are registered in global servers, but the data is not sent to
                  --   them. Rather, clients are redirected to the appropriate local server where
                  --   they will be allowed to register for the queue if they can validate the
                  --   given secret.
                deriving (Show, Read, Eq, Ord, Generic)

instance Serialize Visibility

-- | Information about a remote queue.
data QueueInfo = QueueInfo
                 { qiName       :: QueueName,
                   qiVisibility :: Visibility,
                   qiType       :: QueueType }
               deriving (Show, Read, Eq, Ord, Generic)

instance Serialize QueueInfo

data WriteEnd a = WriteEnd QueueInfo (TChan a)
                deriving (Typeable)
data ReadEnd a = ReadEnd QueueInfo (TChan a)
               deriving (Typeable)

data QueueError = QueueNotFound QueueName
                | QueueAlreadyExists QueueName Visibility
                | NotConnectedToRemote
                | UnexpectedReply
                | TypeMismatch
                | BadDataFormat String
                | OtherError String
                deriving (Show, Generic)

instance Serialize QueueError

instance Error QueueError where
  noMsg = OtherError "No message supplied"
  strMsg = OtherError

data LocalResponse = LocalQueueRegistered QueueInfo (TChan ByteString)
                   | LocalQueueJoined QueueInfo (TChan ByteString)
                   | LocalRemoteFailure QueueError

instance Show LocalResponse where
  show (LocalQueueRegistered qi _) = concat ["LocalQueueRegistered ", show qi]
  show (LocalQueueJoined qi _) = concat ["LocalQueueJoined ", show qi]
  show (LocalRemoteFailure err) = concat ["LocalRemoteFoilure", show err]

class (Serialize a, Typeable a, Show a) => DEQable a where
  fromType :: a -> QueueType
  fromType x = fromTypeRep . typeOf $ x

-- | Raw Bytestring types. This basically represents all queues
--   for which we have no type information, and are just passing
--   on between clients and servers.
newtype Raw = Raw { unRaw :: ByteString }
  deriving (Show, Eq, Ord, Typeable)

instance Serialize Raw where
  put s = mapM_ put (BS.unpack . unRaw $ s)
  get = Raw <$> (getBytes =<< remaining)

instance DEQable Raw
instance DEQable Int
instance DEQable Float
instance DEQable Char
instance DEQable ()
instance (DEQable a) => DEQable [a]
instance (DEQable a, DEQable b) => DEQable (a, b)
instance (DEQable a, DEQable b, DEQable c) => DEQable (a, b, c)
instance (DEQable a, DEQable b, DEQable c, DEQable d) => DEQable (a, b, c, d)
instance (DEQable a, DEQable b, DEQable c, DEQable d, DEQable e) => DEQable (a, b, c, d, e)
instance (DEQable a, DEQable b, DEQable c, DEQable d, DEQable e, DEQable f) => DEQable (a, b, c, d, e, f)

-- Server declarations

-- | Descriptor that tells a server how to begin streaming and accepting queue feeds.
data DataEndpoint = TCP !Int String
                    -- ^ TCP endpoint with given port and hostname
                  | UDP !Int String
                    -- ^ UDP endpoint with given port and hostname
                  | Unix FilePath
                    -- ^ Unix endpoint
                  deriving (Generic)

instance Serialize DataEndpoint

data Command = Join QueueName (Maybe QueueType)
             | Leave QueueToken
             | Info QueueName
             | Stream DataEndpoint

             | Register QueueInfo
             | Unregister QueueToken

             | Data QueueToken ByteString

             | Ping
             deriving Generic -- TODO: It may be more efficient to instantiate Serialize explicitly

instance Serialize Command -- Uses generic functionality

data Response = Ok
              | Fail QueueError
              | Registered QueueToken QueueInfo
              | Joined QueueToken QueueInfo
              | InfoR QueueInfo
              | DataR QueueToken ByteString
              deriving (Show, Generic)

instance Serialize Response

isRootServer :: DEQ -> Bool
isRootServer (DEQ { deqClient = Nothing }) = True
isRootServer _ = False

fromTypeRep :: TypeRep -> QueueType
fromTypeRep rep = let con = typeRepTyCon rep
                  in fromString . concat $
                     [show rep, " ", tyConModule con, " ", tyConPackage con]

getReadEnd :: DEQable a => DEQQueue -> IO (Maybe (ReadEnd a))
getReadEnd (DEQQueue qi chan) = mfix $ \(~(Just x)) ->
  let typeRep = head . typeRepArgs . typeOf $ x
      queueTypeExpected = fromTypeRep typeRep
      queueTypeActual = fromTypeRep . head . typeRepArgs . typeOf $ chan
  in if queueTypeActual == queueTypeExpected
     then do
       chan' <- atomically (dupTChan (unsafeCoerce chan))
       return (Just (ReadEnd qi chan')) -- We handled the type checking in the if statement
     else return Nothing
