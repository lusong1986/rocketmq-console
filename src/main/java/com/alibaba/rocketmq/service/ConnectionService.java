package com.alibaba.rocketmq.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.GroupList;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import com.alibaba.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-16
 */
@Service
public class ConnectionService extends AbstractService {
    static final Logger logger = LoggerFactory.getLogger(ConnectionService.class);

    static final ConsumerConnectionSubCommand consumerConnectionSubCommand =
            new ConsumerConnectionSubCommand();


    public Collection<Option> getOptionsForGetConsumerConnection() {
        return getOptions(consumerConnectionSubCommand);
    }


    @CmdTrace(cmdClazz = ConsumerConnectionSubCommand.class)
    public ConsumerConnection getConsumerConnection(String consumerGroup) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
            return cc;
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }
    
    
	public Table getOnlineConsumerGroupList() throws Throwable {
		Throwable t = null;
		DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
		try {
			defaultMQAdminExt.start();

			final List<String> onlineConsumerGroupNameList = new ArrayList<String>(getOnlineConsumerGroupList(defaultMQAdminExt));
			Collections.sort(onlineConsumerGroupNameList);
			if (onlineConsumerGroupNameList.size() > 0) {
				Table table = new Table(new String[] { "consumer group"}, onlineConsumerGroupNameList.size());
				for (String consumerGroupName : onlineConsumerGroupNameList) {
					Object[] tr = table.createTR();
					tr[0] = consumerGroupName;
					table.insertTR(tr);
				}
				return table;
			} else {
				throw new IllegalStateException("onlineGroupNameList is blank");
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
			t = e;
		} finally {
			shutdownDefaultMQAdminExt(defaultMQAdminExt);
		}
		throw t;
	}
	

	private Set<String> getOnlineConsumerGroupList(DefaultMQAdminExt defaultMQAdminExt) throws RemotingException,
			MQClientException, InterruptedException, MQBrokerException {
		Set<String> groupNameList = new HashSet<String>();
		TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
		if (topicList.getTopicList().size() > 0) {
			for (String topicName : topicList.getTopicList()) {
				GroupList groplist = defaultMQAdminExt.queryTopicConsumeByWho(topicName);
				if (groplist != null && groplist.getGroupList().size() > 0) {
					groupNameList.addAll(groplist.getGroupList());
				}
			}
		}

		Set<String> onlineGroupNameList = new HashSet<String>();
		for (final String consumerGroup : groupNameList) {
			try {
				defaultMQAdminExt.examineConsumeStats(consumerGroup);
				defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
				onlineGroupNameList.add(consumerGroup);
			} catch (Throwable e) {
			}
		}

		return onlineGroupNameList;
	}
    

    static final ProducerConnectionSubCommand producerConnectionSubCommand =
            new ProducerConnectionSubCommand();


    public Collection<Option> getOptionsForGetProducerConnection() {
        return getOptions(producerConnectionSubCommand);
    }


    @CmdTrace(cmdClazz = ProducerConnectionSubCommand.class)
    public ProducerConnection getProducerConnection(String group, String topicName) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            ProducerConnection pc = defaultMQAdminExt.examineProducerConnectionInfo(group, topicName);
            return pc;
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }
}
