from extension import mongo


class TableSelector:

    class Meta:
        db_driver = mongo

    @classmethod
    def get_physical_nodes_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_physical_nodes_table
    
    @classmethod
    def get_submarine_cables_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_submarine_cables_table
    
    @classmethod
    def get_landing_points_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_landing_points_table
    
    @classmethod
    def get_land_cables_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_land_cables_table
    
    @classmethod
    def get_physical_links_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_physical_links_table
    
    @classmethod
    def get_logic_nodes_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_logic_nodes_table
    
    @classmethod
    def get_logic_links_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_logic_links_table
