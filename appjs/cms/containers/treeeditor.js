/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
const {Component, createElement: ce} = require('react'),
    PropTypes = require('prop-types'),
      _ = require('lodash'),
      {connect} = require('react-redux');

const {default: {
    getRelated,
    getEntity,
    jsonFetch,
}} = require('../api');

const {Tree} = require('../components/tree');

const {spinner} = require('../components/fa');


function isleafProp(entity) {
    return Object.defineProperty(
        _.pick(entity, ['eid', 'cw_etype', 'title']),
        'isleaf',
        {
            get() {
                return (this.cw_etype !== 'Section' &&
                        this.cw_etype !== 'CommemoCollection');
            },
        }
    );
}


class TreeEditor extends Component {

    constructor(props) {
        super(props);
        this.topSections = this.props.topSections.toJS();
        this.entity = this.props.entity.toJS();
        this.state = {nodes: null};
    }

    componentDidMount() {
        const promises = this.topSections.map(
            eid => getEntity('section', eid))
        Promise.all(promises).then(entities => {
            const nodes = {};
            entities.forEach(e => {
                nodes[e.eid] = isleafProp(e);
            });
            this.setState({nodes});
        });
    }

    render() {
        const {topSections, entity} = this,
              {eid: currentEid} = entity,
              {nodes} = this.state;
        return ce('div', {id: 'tree-container'},
                  nodes === null ? ce(spinner) : ce(Tree, {
                      ancestors: this.props.ancestors,
                      nodes,
                      topEids: topSections,
                      onFetchChildren(entity) {
                          return getRelated(entity.cw_etype, entity.eid, 'children')
                              .then(children => children.map(isleafProp))
                      },
                      onMove(target) {
                          return jsonFetch('/section', {method: 'post',
                                                        body: JSON.stringify(
                                                            {target: target.eid,
                                                             child: currentEid,
                                                             newOrder: 0})})
                      },
                      entity: this.entity,
                  }));

    }
}
TreeEditor.propTypes = {
    entity: PropTypes.object,
    topSections: PropTypes.object,
    ancestors: PropTypes.array,
}


module.exports = connect(
    function mapStateToProps(state) {
        return {topSections: state.getIn(['model', 'top']),
                ancestors: state.getIn(['model', 'ancestors']),
                entity: state.getIn(['model', 'entity'])};
    }
)(TreeEditor);
