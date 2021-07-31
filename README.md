# rabbitmq
This a package about queue

# consumer
    remoteMessageHandler := service.RemoteMessageHandler{
    Log: log.New(os.Stdout, "", log.LstdFlags),
    }
	queueList := []string{"test-queue-1", "test-queue-2"}
	routingKeyQueue := map[string][]string{}
	routingKeyQueue1 := []string{"test-queue-1"}
	routingKeyQueue2 := []string{"test-queue-2"}
	for index, queue := range queueList {
		if index == 0 {
			routingKeyQueue[queue] = routingKeyQueue1
			continue
		}
		routingKeyQueue[queue] = routingKeyQueue2
	}
	urlRabbit := fmt.Sprintf("amqp://%s:%s@%s:%s", os.Getenv("RABBIT_USER"), os.Getenv("RABBIT_PASS"), os.Getenv("RABBIT_HOST"), os.Getenv("RABBIT_PORT"))
	w := sync.WaitGroup{}
	w.Add(2)
	go func() {
		var rabbitService *service.RabbitService
		rabbitService = service.NewRabbitService(os.Getenv("RABBIT_PRODUCER"), os.Getenv("RABBIT_EXCHANGE"), urlRabbit)
		service.NewRabbitConsumer(rabbitService, queueList, routingKeyQueue, &remoteMessageHandler)
		defer w.Done()
	}()
	w.Wait()

# publisher
	publisherService := service.NewRabbitPublisher(rabbitService)
