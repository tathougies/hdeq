{-# LANGUAGE OverloadedStrings #-}
-- | Configuration module for DEQ Server. Reads in a configuration file using `Data.Configurator`
--   syntax.
module Distributed.DEQ.Server.Config
       ( DEQSettings,
         DEQProperty,

         sPort,
         sHostname)
       where

import Data.Configurator

import Data.Conduit.Network (HostPreference)

newtype DEQServerSettings = DEQServerSettings { config_ :: Config }
newtype DEQServerProperty = DEQServerProperty { property_ :: Name }

type GetProperty a = DEQServerSettings -> a

-- | Load DEQ Server settings from the default locations. Currently, these are:
--
--       * @/etc/deq/server.cfg@
--       * @$(HOME)/deq/server.cfg@
--       * @deq-server.cfg@ (in current directory)
loadDEQServerSettings :: IO DEQSettings
loadDEQServerSettings = load [Optional "/etc/deq/server.cfg",
                              Optional "$(HOME)/deq/server.cfg",
                              Optional "deq-server.cfg"] >>=
                        DEQServerSettings

-- | Get the port that we want to open the server on.
--
--   [___Default___] 9500
--   [___Property Name___] @host.port@
sPort :: GetProperty Int
sPort c = lookupDefault 9500 (config_ c) "host.port"

-- | Get the port over which we'll accept data.
--
--    [___Default___] `sPort + 1`
--    [___Property Name___] @host.dataPort@
sDataPort :: GetProperty Int
sDataPort c = lookupDefault (sPort c + 1) (config_ c) "host.dataPort"

-- | Get the host we'd like to bind the server on.
--
--   [___Default___] `HostAny`
--   [___Property Name___] @host.name@
sHostname :: GetProperty HostPreference
sHostname c = fromString $
              lookupDefault "*" (config_ c) "host.name"