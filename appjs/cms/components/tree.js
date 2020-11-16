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
    PropTypes = require('prop-types')

const {spinner, icon} = require('./fa')

class Node extends Component {
    constructor(props) {
        super(props)
        const expanded = Boolean(this.props.expanded)
        this.state = {expanded, fetching: expanded}
        this.toggleExpanded = this.toggleExpanded.bind(this)
    }

    componentDidMount() {
        if (this.props.expanded) {
            this.fetchChildren()
        }
    }

    fetchChildren() {
        const {onFetchChildren, entity} = this.props
        this.setState({fetching: true})
        onFetchChildren(entity).then(() => this.setState({fetching: false}))
    }

    toggleExpanded() {
        const {expanded} = this.state,
            newState = !expanded
        this.setState({expanded: newState})
        if (newState) {
            this.fetchChildren()
        }
    }

    render() {
        const {expanded, fetching} = this.state,
            {
                renderer,
                entity,
                onFetchChildren,
                ancestors,
                allNodes,
            } = this.props
        if (entity.isleaf) {
            return ce('li', {style: {paddingLeft: '20px'}}, entity.title)
        }
        let child = null
        if (expanded) {
            if (fetching) {
                child = ce(spinner)
            } else {
                child = ce(
                    'ul',
                    null,
                    eidsToNode(entity.children, allNodes, ancestors, {
                        onFetchChildren,
                        renderer,
                    }),
                )
            }
        }
        const titleParts = renderer(entity, expanded)
        return ce(
            'li',
            null,
            ce(
                'span',
                null,
                titleParts,
                ce(
                    'span',
                    {
                        onClick: this.toggleExpanded,
                        className: 'tree-title pointer',
                    },
                    entity.title,
                ),
            ),
            child,
        )
    }
}

Node.propTypes = {
    renderer: PropTypes.func,
    onFetchChildren: PropTypes.func,
    entity: PropTypes.object,
    allNodes: PropTypes.object,
    expanded: PropTypes.bool,
    ancestors: PropTypes.array,
}

class Tree extends Component {
    constructor(props) {
        super(props)
        this.allNodes = Object.assign({}, this.props.nodes)
        this.state = {flag: true} // odd flag used to trigger render
        this.childrenFetcher = this.childrenFetcher.bind(this)
        this.renderNodeTitle = this.renderNodeTitle.bind(this)
    }

    renderNodeTitle(entity, expanded) {
        return [
            ce(icon, {
                key: 'icon-expand',
                name: expanded ? 'caret-down' : 'caret-right',
            }),
            this.props.entity.eid === entity.eid
                ? null
                : ce(
                      'button',
                      {
                          key: 'btn-pin',
                          onClick: () => this.move(entity),
                          className: 'btn btn-default btn-xs',
                      },
                      entity.pinning
                          ? ce(spinner)
                          : ce(icon, {name: 'map-pin'}),
                  ),
        ]
    }

    move(target) {
        this.allNodes[target.eid].pinning = true
        this.triggerUpdate()
        return this.props
            .onMove(target)
            .then(() => {
                const {allNodes} = this,
                    entityEid = this.props.entity.eid,
                    targetEntity = allNodes[target.eid]
                // 1. remove spinner
                targetEntity.pinning = false
                // 2. remove entity from previous parent.children
                for (let eid of Object.keys(allNodes)) {
                    let e = allNodes[eid]
                    if (e.children && e.children.includes(entityEid)) {
                        e.children = e.children.filter(c => c !== entityEid)
                        break
                    }
                }
                // 3. add entity to target.children
                if (Array.isArray(targetEntity.children)) {
                    targetEntity.children.push(entityEid)
                } else {
                    targetEntity.children = [entityEid]
                }
            })
            .then(() => this.triggerUpdate())
    }

    triggerUpdate() {
        this.setState({flag: !this.state.flag})
    }

    childrenFetcher(node) {
        const {allNodes} = this
        return this.props.onFetchChildren(node).then(children => {
            const eids = []
            for (let c of children) {
                if (!allNodes.hasOwnProperty(c.eid)) {
                    allNodes[c.eid] = c
                }
                eids.push(c.eid)
            }
            allNodes[node.eid].children = eids
            this.triggerUpdate()
        })
    }

    render() {
        const {topEids, ancestors} = this.props,
            {allNodes} = this
        return ce(
            'ul',
            null,
            eidsToNode(topEids, allNodes, ancestors, {
                onFetchChildren: this.childrenFetcher,
                renderer: this.renderNodeTitle,
            }),
        )
    }
}

Tree.propTypes = {
    topEids: PropTypes.array.isRequired,
    onFetchChildren: PropTypes.func,
    onMove: PropTypes.func,
    nodes: PropTypes.object,
    ancestors: PropTypes.object,
    entity: PropTypes.object,
}

function eidsToNode(eids, allNodes, ancestors, props) {
    // TODO add ancestors param to compute `expand` props value
    return eids.map(eid =>
        ce(
            Node,
            Object.assign(
                {
                    key: eid,
                    ancestors,
                    entity: allNodes[eid],
                    expanded: ancestors.includes(eid),
                    allNodes,
                },
                props,
            ),
        ),
    )
}

exports.Tree = Tree
exports.Node = Node
