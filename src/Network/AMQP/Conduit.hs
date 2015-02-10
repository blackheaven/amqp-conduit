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
-- Connect to a server, declare a queue and an exchange and setup a callback for messages coming in the queue, then publish a message to our new exchange
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
-- >main :: IO ()
-- >main = hspec $ do
-- >    describe "produce and consume test" $ do
-- >        it "send a message and recieve the message" $ do
-- >            runResourceT $ withAMQPChannel config $ \conn -> do
-- >                sendMsg str $$ amqpSendSink conn "myKey"
-- >            amqp <- createAMQPChannels config 10
-- >            amqp' <- createConsumers amqp $ \(msg,env) -> do
-- >                amqpReceiveSource (msg,env) $$ printMsg
-- >            -- | NOTE: RabbitMQ 1.7 doesn't implement this command.
-- >            -- amqp'' <- pauseConsumers amqp'
-- >            -- amqp''' <- resumeConsumers amqp''
-- >            threadDelay $ 15  * 1000000
-- >            _ <- deleteConsumers amqp'
-- >            return ()
-- >
-- >str :: String
-- >str = "This is a test message"
-- >
-- >config :: AmqpConf
-- >config = AmqpConf "amqp://guest:guest@192.168.59.103:5672/" queue exchange "myKey"
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
    , AmqpURI
    -- * Connection and Channel
    , withAMQPChannel
    , withAMQPChannels
    , createAMQPChannels
    , closeChannels
    , destoryConnection
    -- * Consumer utils
    , createConsumers
    , createConsumers'
    , createConsumer
    , deleteConsumers
    , deleteConsumer
    , pauseConsumers
    , pauseConsumer
    , resumeConsumers
    , resumeConsumer

    , module Network.AMQP
    ) where
import           Control.Exception.Lifted    (bracket)
import           Control.Monad               (replicateM)
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Data.Conduit                (Sink, Source, await, yield)
import           Data.Maybe                  (isJust, isNothing)
import           Data.Text                   (Text)
import           Network.AMQP                (AMQPException (ConnectionClosedException),
                                              Ack (Ack, NoAck), Channel,
                                              Connection, ConsumerTag, Envelope,
                                              ExchangeOpts, Message, QueueOpts,
                                              addConnectionClosedHandler,
                                              bindQueue, cancelConsumer,
                                              closeChannel, closeConnection,
                                              declareExchange, declareQueue,
                                              exchangeName, flow, fromURI,
                                              getMsg, openChannel,
                                              openConnection'', publishMsg,
                                              queueName)
import           Network.AMQP.Lifted         (consumeMsgs)

-- |  Amqp Connection and Channel
data AmqpConn = AmqpConn
    { amqpConn :: Connection
    , amqpChan :: [(Channel, Maybe ConsumerTag)]
    , amqpConf :: AmqpConf
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
type AmqpURI = String

-- |Create a AMQP connection and Channel(s) and run the given action. The connetion and channnels are properly released after the action finishes using it. Note that you should not use the given Connection, channels outside the action since it may be already been released.
withAMQPChannels :: (MonadIO m, MonadBaseControl IO m)
        => AmqpConf
        -- ^ Connection config to the AMQP server.
        -> Int
       -- ^ number of channels to be kept open in the connection.
        -> (AmqpConn -> m a)
        -- ^ Action to be executed that uses the connection.
        -> m a
withAMQPChannels conf num f =
    bracket
        (do
            -- liftIO $ putStrLn "connecting.."
            liftIO $ connect conf (numCheck num))
        (\conn -> do
            -- liftIO $ putStrLn "disconnecting.."
            liftIO $liftIO $ disconnect conn)
        (\conn -> do
            -- liftIO $ putStrLn "calling function"
            f conn)

-- | Same as runAMQP, but only one channel is opened.
withAMQPChannel:: (MonadIO m, MonadBaseControl IO m)
        => AmqpConf
         -- ^ Connection config to the AMQP server.
        -> (AmqpConn -> m a)
        -- ^ Action to be executed that uses the connection.
        -> m a
withAMQPChannel conf = withAMQPChannels conf 1

-- | Create a connection and channels. Note that it's your responsability to properly close the connection and the channels when unneeded. Use withAMQPChannels for an automatic resource control.
createAMQPChannels :: (MonadIO m, MonadBaseControl IO m)
        => AmqpConf
        -- ^ Connection config to the AMQP server.
        -> Int
        -- ^ number of channels to be kept open in the connection
        -> m AmqpConn
createAMQPChannels conf num  = liftIO $ connect conf (numCheck num)


-- Consumer utils
createConsumer :: (MonadIO m, MonadBaseControl IO m)
        => (Channel, Maybe ConsumerTag)
        -> Text
        -> Ack
        -> ((Message, Envelope) -> m ())
        -> m (Channel, Maybe ConsumerTag)
createConsumer (chan, tag) queue ack f =
    case tag of
        Nothing -> do
            tag' <- consumeMsgs chan queue ack f
            return (chan, Just tag')
        Just _ -> return (chan, tag)

deleteConsumer :: MonadIO m
        => (Channel, Maybe ConsumerTag)
        -> m (Channel, Maybe ConsumerTag)
deleteConsumer (chan, tag) =
    case tag of
        Nothing -> return (chan, tag)
        Just s -> do
            liftIO $ putStrLn "cancel cunsumer channel."
            liftIO $ cancelConsumer chan s
            return (chan, Nothing)

createConsumersHelper :: (MonadIO m, MonadBaseControl IO m)
        => AmqpConn
        -> ((Message, Envelope) -> m ())
        -> Ack
        -> m AmqpConn
createConsumersHelper conn f ack = do
    chan <- mapM (\chan -> createConsumer chan qName ack f) chanList
    return $ conn {amqpChan = chan}
    where
        qName = queueName (amqpQueue (amqpConf conn))
        chanList = filter (\(_, tag) -> isNothing tag) (amqpChan conn)

-- | after processing for any message tha you get, automatically acknowledged (see "NoAck" in the "Network.AMQP" module)
createConsumers' :: (MonadIO m, MonadBaseControl IO m)
        => AmqpConn
        -> ((Message, Envelope) -> m ())
        -> m AmqpConn
createConsumers' conn f = createConsumersHelper conn f NoAck

-- | You have to call ackMsg or ackEnv after processing for any message that you get, otherwise it might be delivered again (see "ackMsg" and "ackEnv" in the "Network.AMQP" module)
createConsumers :: (MonadIO m, MonadBaseControl IO m)
        => AmqpConn
        -> ((Message, Envelope) -> m ())
        -> m AmqpConn
createConsumers conn f = createConsumersHelper conn f Ack

-- helper functions
deleteConsumers :: AmqpConn
        -> IO AmqpConn
deleteConsumers conn = do
    chan <- mapM deleteConsumer chanList
    return $ conn {amqpChan = chan}
    where
        chanList = filter (\(_, tag) -> isJust tag) (amqpChan conn)

pauseConsumers :: AmqpConn
        -> IO AmqpConn
pauseConsumers conn = do
    chan <- mapM pauseConsumer chanList
    return $ conn {amqpChan = chan}
    where
        chanList = filter (\(_, tag) -> isJust tag) (amqpChan conn)

pauseConsumer :: (Channel, Maybe ConsumerTag )
        -> IO (Channel, Maybe ConsumerTag)
pauseConsumer chan = flowConsumer chan False

resumeConsumers :: AmqpConn
        -> IO AmqpConn
resumeConsumers conn = do
    chan <- mapM resumeConsumer chanList
    return $ conn {amqpChan = chan}
    where
        chanList = filter (\(_, tag) -> isJust tag) (amqpChan conn)

resumeConsumer :: (Channel, Maybe ConsumerTag)
        -> IO (Channel, Maybe ConsumerTag)
resumeConsumer chan = flowConsumer chan True

flowConsumer :: (Channel, Maybe ConsumerTag)
        -> Bool
        -> IO (Channel, Maybe ConsumerTag)
flowConsumer (chan, tag)  flag=
    case tag of
        Nothing -> return (chan, tag)
        Just _ -> do
            flow chan flag
            return (chan, tag)

closeChannels :: AmqpConn
        -> IO AmqpConn
closeChannels conn = do
    mapM_ (\(chan, _) -> closeChannel chan ) (amqpChan conn)
    return $ conn {amqpChan =[]}

destoryConnection :: AmqpConn
        -> IO ()
destoryConnection conn = do
    addConnectionClosedHandler (amqpConn conn) False (return ())
    closeConnection (amqpConn conn)

-- internal
connect :: AmqpConf
        -> Int
        -> IO AmqpConn
connect conf num = do
    -- make a connection and a channel of the connection.
    conn <- openConnection'' uri
    chan <- replicateM num (initChan conf conn)

    -- set a excetion when closeing the connection
    -- addConnectionClosedHandler conn True (throwIO (ConnectionClosedException "Connection Closed."))
    return $ AmqpConn conn chan conf
    where
        uri = fromURI (amqpUri conf)

initChan :: AmqpConf
        -> Connection
        -> IO (Channel, Maybe ConsumerTag)
initChan conf conn = do
    chan <- openChannel conn

    -- debug
    -- liftIO $ putStrLn ("URI = " ++ (show (amqpUri conf)))
    -- liftIO $ putStrLn ("exchange name = " ++ (show eName))
    -- liftIO $ putStrLn ("ecchange key = " ++ (show (key)))
    -- liftIO $ putStrLn ("queue name= = " ++ (show (qName)))

    -- declare exchange, a queue and binding
    declareExchange chan (amqpExchange conf)
    _ <- declareQueue chan (amqpQueue conf)
    bindQueue chan qName eName key

    return (chan, Nothing)
    where
        qName = queueName (amqpQueue conf)
        key = amqpExchanKey conf
        eName = exchangeName (amqpExchange conf)


disconnect :: AmqpConn
        -> IO ()
disconnect conn = do
    -- mapM_ (\(chan, _) -> closeChannel chan) (amqpChan conn)
    closeConnection (amqpConn conn)

send :: AmqpConn
        -> ExchangeKey
        -> Message
        -> IO ()
send conn key msg = do
    -- liftIO $ putStrLn ("key = " ++ show key ++ ", msg = " ++ show msg)
    publishMsg
        channel
        (exchangeName (amqpExchange (amqpConf conn))) key msg
    where
        -- | TODO:
        channel = fst $ head $ (amqpChan conn)

numCheck :: Int -> Int
numCheck int
    | int <= 0 = 1
    | int > 0 = int
    | otherwise = 1

-- | this is AMQP consumer, if you use it with createConsumers, You have to call ackMsg or ackEnv after processing for any message that you get, otherwise it might be delivered again (see "ackMsg" and "ackEnv" in the "Network.AMQP" module)
amqpReceiveSource :: (Monad m, MonadIO m)
        => (Message, Envelope)
        -> Source m (Message, Envelope)
amqpReceiveSource (msg, env) = loop
    where
        loop = do
            yield (msg, env)
            loop

-- | this is AMQP producer.
amqpSendSink :: (Monad m, MonadIO m)
        => AmqpConn
        -> ExchangeKey
        -> Sink Message m ()
amqpSendSink conn key = loop
    where
        loop = await >>= maybe (return ()) (\v -> (liftIO $ send conn key v) >> loop)




