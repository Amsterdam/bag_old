"""
collect and validate bag, brk, gebieden and wkpb table counts
"""
import logging
from django.db import connection


LOG = logging.getLogger(__name__)

TABLE_TARGETS = [
    (9033, 0, "bag_bouwblok"),
    (100, 0, "bag_bron"),
    (481, 0, "bag_buurt"),
    (99, 0, "bag_buurtcombinatie"),
    (2, 0, "bag_eigendomsverhouding"),
    (19, 0, "bag_financieringswijze"),
    (22, 6, "bag_gebiedsgerichtwerken"),
    (320, 0, "bag_gebruik"),
    (515178, 0, "bag_gebruiksdoel"),
    (1, 0, "bag_gemeente"),
    (53, 9, "bag_grootstedelijkgebied"),
    (6, 0, "bag_ligging"),
    (2913, 0, "bag_ligplaats"),
    (5, 0, "bag_locatieingang"),
    (517683, 0, "bag_nummeraanduiding"),
    (6191, 0, "bag_openbareruimte"),
    (184345, 0, "bag_pand"),
    (44, 0, "bag_redenafvoer"),
    (44, 0, "bag_redenopvoer"),
    (8, 0, "bag_stadsdeel"),
    (323, 0, "bag_standplaats"),
    (47, 5, "bag_status"),
    (9, 0, "bag_toegang"),
    (2, 0, "bag_unesco"),
    (512154, 0, "bag_verblijfsobject"),
    (513315, 0, "bag_verblijfsobjectpandrelatie"),
    (1, 20, "bag_woonplaats"),  # Er mogen meer woonplaatsen worden geleverd
]


def sql_count(table):

    count = 0

    with connection.cursor() as c:
        c.execute('SELECT COUNT(*) FROM {}'.format(connection.ops.quote_name(table)))
        row = c.fetchone()
        count += row[0]
        # LOG.debug('COUNT %-6s %s', count, table)

    return count


def check_table_counts(table_data: list):
    """
    Given list with tuples of count - table name
    check if current table counts are close
    """
    error = False
    all_msg = ("Table count errors \n"
               "Count ,   Target,  Deviation-Allowed,      Table,           Status \n")

    for target, override, table in table_data:
        count = sql_count(table)
        delta = abs(count - target)
        deviation = int(0.05 * target)
        if override:
            deviation = override
        if delta > deviation or count == 0:
            status = '<FAIL>'
            error = True
        else:
            status = '< OK >'

        msg = f"{count:>6} ~= {target:<11} {delta:>6}-{deviation:<6} {table:<42} {status} \n"
        all_msg += msg

    if error:
        LOG.error(msg)
        raise ValueError(all_msg)
    else:
        LOG.debug(all_msg)


def check_table_targets():
    """
    Check if tables have a specific count
    """
    LOG.debug('Validating table counts..')

    # Count,   table
    check_table_counts(TABLE_TARGETS)
