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
'use strict';
const {Component, createElement: ce, Children} = require('react'),
    PropTypes = require('prop-types');

const {connect} = require('react-redux');

const {togglePanel} = require('../actions'),
      {icon} = require('../components/fa'),
      {AlertError} = require('./error'),
      PublishBtn = require('./publish'),
      OnHomePageIcon = require('./on-homepage'),
      {FaImport} = require('./faimport'),
      {TreeLink,
       EditServiceListLink,
       EditFormLink,
       EditIndexLink,
       displayRelated,
       AlertLink,
       DeleteLink,
       TodoLink,
       AddServiceLink,
       AddCWUserLink,
       ConsultationLink,
       SearchCWUsersLink,
       SearchFaTasksLink,
       FAMonitoringBordLink,
       EditHomePageMetataLink,
       SameAsLink,
       GroupAuthLink,
       PublishTaskLink,
       AddLink} = require('./index');

const ACTIONS = {
    tree: TreeLink,
    'edit-form': EditFormLink,
    'edit-index': EditIndexLink,
    'mark-home': OnHomePageIcon,
    'edit-service-list': EditServiceListLink,
    relation: displayRelated,
    add: AddLink,
    alert: AlertLink,
    publish: PublishBtn,
    delete: DeleteLink,
    todos: TodoLink,
    'add-service': AddServiceLink,
    'add-user': AddCWUserLink,
    'fa-import': FaImport,
    'cwusers': SearchCWUsersLink,
    'fa-tasks': SearchFaTasksLink,
    'fa-bord': FAMonitoringBordLink,
    'consultation-link': ConsultationLink,
    'homepage-metadata': EditHomePageMetataLink,
    'fa-publish-task': PublishTaskLink,
    'edit-same-as': SameAsLink,
    'group-authorities': GroupAuthLink,
};


class App extends Component {

    constructor(props) {
        super(props);
        this.actions = [];
        for (let l of Array.from(document.querySelectorAll('link[rel=cms-js]'))) {
            this.actions.push(l.getAttribute('url'));
        }
    }

    toggleParentWidth(showPanel) {
        if (showPanel) {
            this.wrapper.parentElement.classList.add('panel-unfolded');
        } else {
            this.wrapper.parentElement.classList.remove('panel-unfolded');
        }
    }

    componentDidMount() {
        this.toggleParentWidth(this.props.showPanel);
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        this.toggleParentWidth(nextProps.showPanel);
    }

    render() {
        const {togglePanel, showPanel} = this.props;
        const children = [
            ce('h2', {}, "Outils d'édition"),
            ce('hr'),
            ce('h2', {title: 'déplier/replier le panneau'},
               ce(icon, {onClick: togglePanel,
                         name: `toggle-${showPanel ? 'on' : 'off'}`,
                         className: 'pointer'})),
        ];
        for (const actionname of this.actions) {
            children.push(
                ce('hr'),
                ce(ACTIONS[actionname])
            );
        }
        return ce('div', {className: `row ${showPanel ? 'panel-unfolded' : ''}`,
                          ref: n => this.wrapper = n},
                  ce('div', {className: `col-xs-${showPanel ? '2' : '12'}`,
                             id: 'toolbar'},
                     ...children),
                  ce('div', {className: 'col-xs-10', id: 'mainpanel'},
                     ce(AlertError),
                     ...Children.toArray(this.props.children)));
    }
}
App.propTypes = {
    togglePanel: PropTypes.func.isRequired,
    showPanel: PropTypes.bool.isRequired,
    children: PropTypes.object,
}


module.exports = connect(
    function mapStateToProps(state) {
        return {showPanel: state.getIn(['app', 'showPanel'])};
    },
    function mapDispatchToProps(dispatch) {
        return {
            togglePanel() {
                dispatch(togglePanel());
            },
        };
    }
)(App);
module.exports.App = App;
