{-# LANGUAGE OverloadedStrings #-}
-- | Configuration module for DEQ Server. Reads in a configuration file using `Data.Configurator`
--   syntax.
module Distributed.DEQ.Config
       ( DEQSettings,
         DEQProperty,
         HostType(..),

         loadDEQSettings,
         loadDEQSettings',

         sHostDisabled,
         sHostUnixSocket,
         sHostPort,
         sHostName,
         sHostType,

         sClientDisabled,
         sClientType,
         sClientUnixSocket,
         sClientPort,
         sClientHostName)
       where

import Prelude hiding (lookup)

import Control.Applicative

import Data.Configurator (Worth(..), load, getMap)
import Data.Configurator.Types (Name, Value, Configured(convert))
import Data.HashMap.Strict (HashMap, lookup)
import Data.String
import Data.Monoid

import Data.Conduit.Network (HostPreference)

newtype DEQSettings = DEQSettings { config_ :: HashMap Name Value }
newtype DEQProperty = DEQProperty { property_ :: Name }

data HostType = UNIX | TCP deriving (Show, Read, Eq, Ord, Enum)

type GetProperty a = DEQSettings -> a

-- | Load DEQ  settings from the default locations. Currently, these are:
--
--       * @/etc/deq/deq.cfg@
--       * @$(HOME)/deq/deq.cfg@
--       * @deq.cfg@ (in current directory)
loadDEQSettings :: IO DEQSettings
loadDEQSettings = DEQSettings <$>
                  (getMap =<<
                   load [Optional "/etc/deq/deq.cfg",
                         Optional "$(HOME)/deq/deq.cfg",
                         Optional "deq.cfg"])

loadDEQSettings' :: [FilePath] -> HashMap Name Value -> IO DEQSettings
loadDEQSettings' files defaults = DEQSettings . (<> defaults) <$>
                                  (getMap =<<
                                   load (map Optional files))

lookupDefault' :: Configured a => a -> Name -> HashMap Name Value -> a
lookupDefault' def name hm = case lookup name hm of
                               Just x -> case convert x of
                                           Just x -> x
                                           Nothing -> def
                               Nothing -> def

-- | Get whether the host is disabled.
--
--   [___Default____] `True`
--   [___Property Name___] @host.disabled@
sHostDisabled :: GetProperty Bool
sHostDisabled c = lookupDefault' True "host.disabled" (config_ c)

-- | Get whether the client is disabled
--
--   [___Default___] `False`
--   [___Property Name___] @client.disabled@
sClientDisabled :: GetProperty Bool
sClientDisabled c = lookupDefault' False "client.disabled" (config_ c)

-- | Get which kind of host to do (unix or TCP)
--
--  [___Default___] `UNIX`
--  [___Property Name___] @host.type@
sHostType :: GetProperty HostType
sHostType c = case (lookupDefault' "UNIX" "host.type"  (config_ c) :: String) of
                "UNIX" -> UNIX
                "TCP"  -> TCP
                _      -> error "Unknown host type"

-- | Get the name of the default Unix socket name
--
--   [___Default___] `/etc/deq/deq-socket`
--   [___Property Name___] @host.unixSocket@
sHostUnixSocket :: GetProperty FilePath
sHostUnixSocket c = lookupDefault' "/etc/deq/deq-socket" "host.unixSocket" (config_ c)

-- | Get the port that we want to open the server on.
--
--   [___Default___] 9500
--   [___Property Name___] @host.port@
sHostPort :: GetProperty Int
sHostPort c = lookupDefault' 9500 "host.port" (config_ c)

-- | Get the host we'd like to bind the server on.
--
--   [___Default___] `HostAny`
--   [___Property Name___] @host.name@
sHostName :: GetProperty HostPreference
sHostName c = fromString $
              lookupDefault' "*" "host.name" (config_ c)

-- | Get the type of client (Unix or TCP)
--
--   [___Default___] `UNIX`
--   [___Property Name___] @client.type@
sClientType :: GetProperty HostType
sClientType c = case (lookupDefault' "UNIX" "client.type" (config_ c) :: String) of
                  "UNIX" -> UNIX
                  "TCP"  -> TCP
                  _      -> error "Unknown host type"

-- | Get the default unix socket name for the client
--
--   [___Default___] `/etc/deq/deq-socket`
--   [___Property Name___] @client.unixSocket@
sClientUnixSocket :: GetProperty FilePath
sClientUnixSocket c = lookupDefault' "/etc/deq/deq-socket" "client.unixSocket" (config_ c)

sClientPort :: GetProperty Int
sClientPort c = lookupDefault' 9500 "client.port" (config_ c)

sClientHostName :: GetProperty String
sClientHostName c = case lookupDefault' "" "client.hostName" (config_ c) of
                      "" -> error "Client host name required for TCP clients"
                      x -> x