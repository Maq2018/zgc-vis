from extension import mongo


class TableSelector:

    class Meta:
        db_driver = mongo

    @classmethod
    def get_nodes_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_nodes_table
    
    @classmethod
    def get_clusters_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_clusters_table
    
    @classmethod
    def get_asrank_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_asrank_table
    
    @classmethod
    def get_phy_links_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_phy_link_table
    
    @classmethod
    def get_logic_nodes_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_logic_node_table
    
    @classmethod
    def get_logic_links_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_logic_link_table
