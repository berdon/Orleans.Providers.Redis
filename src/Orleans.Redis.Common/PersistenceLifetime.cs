using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Configuration
{
    public enum PersistenceLifetime
    {
        /// <summary>
        /// Indicates persistence should associated with the service lifetime
        /// and thus should survive deployment and redeployment. This is tied to
        /// the Orlean's Service ID.
        /// </summary>
        ServiceLifetime = 0,

        /// <summary>
        /// Indicates persistence should be associated with the cluster lifetime
        /// and should be unique between deployment and redeployment. This is
        /// tied to the Orlean's Cluster ID.
        /// </summary>
        ClusterLifetime = 1
    }
}
