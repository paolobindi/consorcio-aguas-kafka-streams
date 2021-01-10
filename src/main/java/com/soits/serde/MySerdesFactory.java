package com.soits.serde;

import java.rmi.registry.Registry;

public class MySerdesFactory
{
    public static QbalanceSerde qbalanceSerde()
    {
        return new QbalanceSerde();
    }

    public static RegistrySerde registrySerde()
    {
        return new RegistrySerde();
    }

    public static QBalanceRegistryResultSerde qBalanceRegistryResultSerde()
    {
        return new QBalanceRegistryResultSerde();
    }
}
