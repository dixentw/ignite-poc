package spring.boot.webflux.template.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@Slf4j
public class IgniteConfig {

    @Bean
    public Ignite igniteClient() {
        // The node will be started as a client node.
        IgniteConfiguration cfg = new IgniteConfiguration();
        //cfg.setClientMode(true);
        //cfg.setPeerClassLoadingEnabled(true);
        //cfg.setDeploymentMode(DeploymentMode.SHARED);

        // Setting up an IP Finder to ensure the client can locate the servers.
        KubernetesConnectionConfiguration conf = new KubernetesConnectionConfiguration();
        conf.setNamespace("sn-push");
        conf.setServiceName("ignite-client-service");
        TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder(conf);
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
        Ignite client = Ignition.start(cfg);

        log.info("client joined:, {}", client.cluster().nodes());
        return client;
    }
}
