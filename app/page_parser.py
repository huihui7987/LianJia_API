import logging
from pathlib import Path

from pyquery import PyQuery as pq

from config import config
from lian_jia import Community
from util.orm import Session

DATA_DIR = Path(__file__).parent.joinpath('../data/').resolve()


def parse_community_detail(community_id):
    save_file = DATA_DIR.joinpath(u'{0}.html'.format(community_id))

    if save_file.exists():
        d = pq(save_file.read_text(encoding='utf-8'))

        keys = [e.text.rstrip('：') for e in d('span.hdic_key')]
        values = [e.text for e in d('span.hdic_value')]
        detail = dict(zip(keys, values))

        if not detail:
            logging.warning(u'# 页面已找到, 但其中未发现小区详情信息, community_id={0}'.format(community_id))

        return detail

    else:
        logging.error(u'# 详情页面不存在, community_id={0}'.format(community_id))


def parse_all_communities(city_id):
    db_session = Session()

    communities = db_session.query(Community).filter(
        Community.city_id == city_id,
        Community.detail == None,
        Community.page_fetched_at != None
    ).all()

    total_count = len(communities)
    logging.info(u'city_id={0}, 待分析={1}'.format(city_id,total_count))

    for i, a_community in enumerate(communities):
        detail = parse_community_detail(a_community.id)

        if detail:
            a_community.detail = detail
            '''
            a_community.交易权属 = detail['交易权属']
            a_community.车位配比 = detail['车位配比']
            a_community.楼栋总数 = detail['楼栋总数']
            a_community.产权年限 = detail['产权年限']
            a_community.房屋总数 = detail['房屋总数']
            a_community.建筑年代 = detail['建筑年代']
            a_community.供暖类型 = detail['供暖类型']
            '''


        if (i + 1) % 100 == 0 or (i == total_count - 1):
            logging.info(u'进度={0}/{1}, 剩余={2}'.format(i + 1,total_count,total_count - i - 1))
            db_session.commit()

    logging.info('已全部分析完成.')
    db_session.close()


def main():
    parse_all_communities(config.city_id)


if __name__ == '__main__':
    main()
