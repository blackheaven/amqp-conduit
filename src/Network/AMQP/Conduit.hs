{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeOperators         #-}
-- |
-- Conduit bindings for AMQP (see amqp package) https://hackage.haskell.org/package/amqp
--
-- Create a AMQP connection, a channel, declare a queue and an exchange
-- and run the given action.
--
-- /Example/:
--
-- Connect to a server, declare a queue and an exchange and setup a callback for messages coming in on the queue. Then publish a single message to our new exchange
--
-- >{-# LANGUAGE OverloadedStrings #-}
-- >
-- >import           Control.Concurrent           (threadDelay)
-- >import           Control.Monad.IO.Class       (MonadIO, liftIO)
-- >import           Control.Monad.Trans.Resource (runResourceT)
-- >import qualified Data.ByteString.Lazy.Char8   as BL
-- >import           Data.Conduit
-- >import           Network.AMQP
-- >import           Network.AMQP.Conduit
-- >import           Test.Hspec
-- >
-- >
-- >main :: IO ()
-- >main = hspec $ do
-- >    describe "produce and consume test" $ do
-- >        it "send a message and recieve the message" $ do
-- >            runResourceT $ withChannel config $ \conn -> do
-- >                sendMsg str $$ amqpSendSink conn "myExchange" "myKey"
-- >            amqp <- createChannel config
-- >            amqp' <- createConsumer amqp "myQueue" Ack $ \(msg,env) -> do
-- >                amqpReceiveSource (msg,env) $$ printMsg
-- >            -- | NOTE: RabbitMQ 1.7 doesn't implement this command.
-- >            -- amqp'' <- pauseConsumers amqp'
-- >            -- amqp''' <- resumeConsumers amqp''
-- >            threadDelay $ 15  * 1000000
-- >            _ <- deleteConsumer amqp'
-- >            return ()
-- >
-- >str :: String
-- >str = "This is a test message"
-- >
-- >config :: AmqpConf
-- >config = AmqpConf "amqp://guest:guest@localhost:5672/" queue exchange "myKey"
-- >    where
-- >        exchange = newExchange {exchangeName = "myExchange", exchangeType = "direct"}
-- >        queue = newQueue {queueName = "myQueue"}
-- >
-- >sendMsg :: (Monad m, MonadIO m) => String -> Source m Message
-- >sendMsg msg = do
-- >    yield (newMsg {msgBody = (BL.pack msg),msgDeliveryMode = Just Persistent} )
-- >
-- >printMsg :: (Monad m, MonadIO m) => Sink (Message, Envelope) m ()
-- >printMsg = do
-- >    m <- await
-- >    case m of
-- >       Nothing -> printMsg
-- >       Just (msg,env) -> do
-- >           liftIO $ ackEnv env
-- >           liftIO $ (BL.unpack $ msgBody msg) `shouldBe` str
-- >           liftIO $ putStrLn $ "received message: " ++ (BL.unpack $ msgBody msg)
-- >           -
-- >


module Network.AMQP.Conduit (
    -- * Conduit
      amqpReceiveSource
    , amqpSendSink
    -- * Data type
    , AmqpConf (..)
    , AmqpConn (..)
    , ExchangeKey
    , Exchange
    , QueueName
    , AmqpURI
    -- * Connection and Channel
    , withChannel
    , createConnectionChannel
    , destoryConnection
    -- * Exchange and Queue utils
    , createQueue
    , createExchange
    , bindQueueExchange
    -- * Consumer utils
    , createConsumer
    , deleteConsumer
    , pauseConsumer
    , resumeConsumer
    ) where

import           Control.Exception           (throwIO)
import           Control.Exception.Lifted    (bracket)
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Data.Conduit                (Sink, Source, await, yield)
import           Data.Text                   (Text)
import           Network.AMQP                (AMQPException (ConnectionClosedException),
                                              Ack, Channel, Connection,
                                              ConsumerTag, Envelope,
                                              ExchangeOpts, Message, QueueOpts,
                                              addConnectionClosedHandler,
                                              bindQueue, cancelConsumer,
                                              closeConnection, consumeMsgs,
                                              declareExchange, declareQueue,
                                              exchangeName, flow, fromURI,
                                              openChannel, openConnection'',
                                              publishMsg, queueName)

-- |  Amqp Connection and Channel
data AmqpConn = AmqpConn
    { amqpConn :: Connection
    , amqpChan :: (Channel, Maybe ConsumerTag)
    }

-- | Amqp connection configuration. queue name, exchange name, exchange key name, and amqp URI.
data AmqpConf = AmqpConf
    {
     -- | Connection string to the database.
      amqpUri       :: AmqpURI
    , amqpQueue     :: QueueOpts
    , amqpExchange  :: ExchangeOpts
    , amqpExchanKey :: ExchangeKey
    }

type ExchangeKey = Text
type Exchange = Text
type QueueName = Text
type AmqpURI = String

-- |Create a AMQP connection and a channel and run the given action. The connetion and channnel are properly released after the action finishes using it. Note that you should not use the given Connection, channel outside the action since it may be already been released.
withChannel:: (MonadIO m, MonadBaseControl IO m)
        => AmqpConf
         -- ^ Connection config to the AMQP server.
        -> (AmqpConn -> m a)
        -- ^ Action to be executed that uses the connection.
        -> m a
withChannel conf f = do
    bracket
        (do
            -- liftIO $ putStrLn "connecting.."
            liftIO $ connect (amqpUri conf))
        (\conn -> do
            -- liftIO $ putStrLn "disconnecting.."
            liftIO $ disconnect conn)
        (\conn -> do
            -- liftIO $ putStrLn "calling function"
            f conn)


-- | Create a connection and a channel. Note that it's your responsability to properly close the connection and the channels when unneeded. Use withAMQPChannels for an automatic resource control.
createConnectionChannel :: AmqpConf
        -- ^ Connection config to the AMQP server.
        -> IO AmqpConn
createConnectionChannel conf = connect $ amqpUri conf

-- | Close a connection
destoryConnection :: AmqpConn
        -> IO ()
destoryConnection conn = do
    addConnectionClosedHandler (amqpConn conn) False (return ())
    closeConnection (amqpConn conn)

-- Consumer utils
createConsumer ::  AmqpConn
        -> QueueName
        -> Ack
        -> ((Message, Envelope) -> IO ())
        -> IO AmqpConn
createConsumer conn queue ack f = do
    tag' <- getTag chan
    return $ conn {amqpChan =(chan, Just tag')}
    where
    getTag chan' = consumeMsgs chan' queue ack f
    chan = fst $ amqpChan conn

deleteConsumer :: AmqpConn -> IO AmqpConn
deleteConsumer conn =
    case tag of
        Nothing -> return conn
        Just s -> do
            putStrLn "cancel cunsumer channel."
            cancelConsumer chan s
            return $ conn {amqpChan = (chan, Nothing)}
    where
        (chan, tag) = amqpChan conn

pauseConsumer :: AmqpConn
        -> IO AmqpConn
pauseConsumer chan = flowConsumer chan False

resumeConsumer :: AmqpConn
        -> IO AmqpConn
resumeConsumer chan = flowConsumer chan True

flowConsumer :: AmqpConn
        -> Bool
        -> IO AmqpConn
flowConsumer conn flag =
    case tag of
        Nothing -> return conn
        Just _ -> do
            flow chan flag
            return conn
    where
        (chan, tag) = amqpChan conn

-- utils
--
createQueue :: AmqpConf -> AmqpConn -> IO (QueueName, Int, Int)
createQueue conf conn = declareQueue (fst $ amqpChan conn) (amqpQueue conf)

createExchange :: AmqpConf -> AmqpConn -> IO ()
createExchange conf conn = declareExchange (fst $ amqpChan conn) (amqpExchange conf)

bindQueueExchange :: AmqpConf -> AmqpConn -> IO ()
bindQueueExchange conf conn =
    bindQueue (fst $ amqpChan conn) (queueName (amqpQueue conf)) (exchangeName (amqpExchange conf)) (amqpExchanKey conf)



-- internal
connect :: AmqpURI
        -> IO AmqpConn
connect uri = do
    -- make a connection and a channel of the connection.
    conn <- openConnection'' $ fromURI uri
    chan <- openChannel conn

    -- set a excetion when closeing the connection
    addConnectionClosedHandler conn True (throwIO (ConnectionClosedException "Connection Closed."))
    return $ AmqpConn conn (chan, Nothing)

disconnect :: AmqpConn
        -> IO ()
disconnect conn = do
    closeConnection (amqpConn conn)

sendMsg :: AmqpConn
         -> Exchange
         -> ExchangeKey
         -> Message
         -> IO ()
sendMsg conn exchange key msg = do
    publishMsg chan exchange key msg
    where
        chan = fst (amqpChan conn)

-- | Source as consuming data pushed.
amqpReceiveSource :: MonadIO m
        => (Message, Envelope)
        -> Source m (Message, Envelope)
amqpReceiveSource (msg, env) = loop
    where
        loop = do
            yield (msg, env)
            loop

-- | Sink as sending data.
amqpSendSink :: MonadIO m
        => AmqpConn
        -> Exchange
        -> ExchangeKey
        -> Sink Message m ()
amqpSendSink conn exchange key = loop
    where
        loop = await >>= maybe (return ()) (\v -> (liftIO $ sendMsg conn exchange key v) >> loop)
