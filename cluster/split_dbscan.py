import numpy as np
from sklearn.cluster import DBSCAN
from firedata._utils import ModisGrid, FireDate

class SplitDBSCAN(DBSCAN):
    def __init__(
        self,
        eps=0.5,
        edge_eps=0.5,
        split_dim=0,
        min_samples=5,
        metric="euclidean",
    ):
        super().__init__(
                eps=eps,
                min_samples=min_samples,
                metric=metric,
                )
        self.eps = eps
        self.edge_eps = edge_eps
        self.split_dim = split_dim,

    def split(self, X):
        """Splits clusters into completed and active parts.
        The group membership of active points may change when
        clustering the following chunk or with influx of new data.

        Args:
            X : {array-like} of shape (n_samples, n_features),
            Training instances used when invocing the fit method of
            the self instance.

        Returns:
            active_mask : (bool) a mask with True values indicating
            self.labels_ of active clusters.
        """
        # chunk_edge represents max value in chunk along the 
        # split_dimension
        chunk_edge = X[:, self.split_dim].max()
        # whithin reach is a mask of samples which are within edge_eps
        # distance from the chunk edge
        within_reach = X[:, self.split_dim] >= (chunk_edge - self.edge_eps)
        # unique labels of all within reach samples
        print(within_reach.shape, self.labels_.shape)
        active_labels = np.unique(self.labels_[within_reach.squeeze(axis=1)])
        # mask indicating all within reach self.labels_
        active_mask = np.isin(self.labels_, active_labels)
        return active_mask

    def cluster_dataframe(self, dfr):
        """Convenience method to cluster dataset passed as pandas DataFrame (dfr).
        Must contain longitude, latitude and date columns.
        """
        indx, indy = ModisGrid.modis_sinusoidal_grid_index(dfr.longitude, dfr.latitude)
        day_since = FireDate.days_since(dfr.date)
        ars = np.column_stack([day_since, indx, indy])
        self.fit(ars)
        active_mask = self.split(ars)
        return self.labels_.astype(int), active_mask
